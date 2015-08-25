'use strict';

let throng = require('throng');

const WORKERS = parseInt(process.env.WEB_CONCURRENCY || 1, 10);
const port = parseInt(process.env.PORT || 3000, 10);

if (process.env.REDIS_URL) {
  let redis = require('redis-url').connect(process.env.REDIS_URL);
}
else{
  let redis = false;
}

const LIMIT_REQ = parseInt(process.env.LIMIT_REQ || 5, 10);
const EXPIRE_REQ = parseInt(process.env.EXPIRE_REQ || 10, 10);

const DEBUG = !!parseInt(process.env.DEBUG, 10);
const FLOOD_PROTECTION = !!parseInt(process.env.FLOOD_PROTECTION, 10);

const DIGIT_REG_EXP = /[\d\.]+/;
const UNDERSCORE_REG_EXP = /_/g;

if (require.main === module) {
  throng(start, {
    workers: WORKERS,
    lifetime: Infinity
  });
} else {
  start();
}

function start() {
  console.log("Running start function");
  let _ = require('lodash');
  let assert = require('assert');
  let app = module.exports = require('express')();
  let through = require('through');
  let logfmt = require('logfmt');
  let urlUtil = require('url');
  let StatsD = require('node-statsd');
  let basicAuth = require('basic-auth');
  let statsd = new StatsD(parseStatsdUrl(process.env.STATSD_URL));
  let allowedApps = loadAllowedAppsFromEnv();

  function startsWithSampleSharp (_, key) {
    return key.startsWith('sample#');
  };

  if (DEBUG) {
    console.log('Allowed apps', allowedApps);
    statsd.send = wrap(statsd.send.bind(statsd), console.log.bind(null, 'Intercepted: statsd.send(%s):'));
  }

  const routerMetricsKeys = ['dyno', 'method', 'status', 'path', 'host', 'code', 'desc', 'at'];

  let metricsVariants = {
    dynoMetrics: function (line, prefix, defaultTags) {
      let tags = _.union(tagsToArr({ dyno: line.source }), defaultTags);
      _.forEach(_.pick(line, startsWithSampleSharp), function (value, key) {
        statsd.histogram(prefix + 'heroku.dyno.' + key.split('#')[1].replace(UNDERSCORE_REG_EXP, '.'), extractNumber(value), tags);
      });
    },
    routerMetrics: function (line, prefix, defaultTags) {
      let tags = _.union(tagsToArr(_.pick(line, routerMetricsKeys)), defaultTags);
      statsd.histogram(prefix + 'heroku.router.request.connect', extractNumber(line.connect), tags);
      statsd.histogram(prefix + 'heroku.router.request.service', extractNumber(line.service), tags);
      if (line.at === 'error') {
        statsd.increment(prefix + 'heroku.router.error', 1, tags);
      }
    },
    postgresMetrics: function (line, prefix, defaultTags) {
      let tags = _.union(tagsToArr({ source: line.source }), defaultTags);
      let metrics = _.pick(line, startsWithSampleSharp);
      _.forEach(metrics, function (value, key) {
        statsd.histogram(prefix + 'heroku.postgres.' + key.split('#')[1], extractNumber(value), tags);
        // TODO: Use statsd counters or gauges for some postgres metrics (db size, table count, ..)
      });
    }
  };

  app.use(function authenticate (req, res, next) {
    let auth = basicAuth(req) || {};
    let app = allowedApps[auth.name];
    if (app !== undefined && app.password === auth.pass) {
      req.defaultTags = app.tags;
      req.prefix = app.prefix;
      req.appName = auth.name;
      next();
    } else {
      res.status(401).send('Unauthorized');
      if (DEBUG) {
        console.log('Unauthorized access by %s', auth.name);
      }
    }
  });

  app.use(logfmt.bodyParserStream());

  app.post('/', function (req, res) {
    if (req.body) {
      req.body.pipe(
        through(function (line) {
          processLine(req.appName, line, req.prefix, req.defaultTags);
        })
      );
    }
    req.once('end', function() {
      res.send('OK');
    });
  });

  app.listen(port, function () {
    console.log('Server listening on port ' + port);
  });


  function floodProtected (variant, appNameKey, line, prefix, defaultTags) {
    if (!FLOOD_PROTECTION) {
      return metricsVariants[variant](line, prefix, defaultTags);
    }
    if (DEBUG) {
      console.log('Protection for app: ', appNameKey);
    }
    if (appNameKey !== undefined && redis !== false) {
      redis.get(appNameKey + '.' + variant, function (err, buffer) {
        if (err) {
          if (DEBUG) {
            console.error('Redis get error:', err);
          }
          return metricsVariants[variant](line, prefix, defaultTags);
        }
        if (buffer >= LIMIT_REQ) {
          redis.del(appNameKey + '.' + variant, function (err) {
            if (DEBUG) {
              console.error('Redis del error:', err);
            }
          });
          // flood-gate is open
          return metricsVariants[variant](line, prefix, defaultTags);
        } else {
          redis.incr(appNameKey + '.' + variant, function (err) {
            if (DEBUG) {
              console.error('Redis incr error:', err);
            }
            redis.expire(appNameKey + '.' + variant, EXPIRE_REQ, function (err) {
              if (DEBUG) {
                console.error('Redis expire error:', err);
              }
            });
          });
          // flood-protected = do nothing
          return;
        }
      });
    }
    else {
      metricsVariants[variant](line, prefix, defaultTags);
    }
  }

  /**
   * Matches a line against a rule and processes it
   * @param {object} line
   */
  function processLine (appName, line, prefix, defaultTags) {
    // Dyno metrics
    if (hasKeys(line, ['heroku', 'source', 'dyno'])) {
      if (DEBUG) {
        console.log('Processing dyno metrics');
      }
      floodProtected('dynoMetrics', appName, line, prefix, defaultTags);
    }

    // Router metrics
    else if (hasKeys(line, ['heroku', 'router', 'path', 'method', 'dyno', 'status', 'connect', 'service', 'at'])) {
      if (DEBUG) {
        console.log('Processing router metrics');
      }
      floodProtected('routerMetrics', appName, line, prefix, defaultTags);
    }

    // Postgres metrics
    else if (hasKeys(line, ['source', 'heroku-postgres'])) {
      if (DEBUG) {
        console.log('Processing postgres metrics');
      }
      floodProtected('postgresMetrics', appName, line, prefix, defaultTags);
    }

    // Scaling event
    else if (line.api === true && line.Scale === true) {
      if (DEBUG) {
        console.log('Processing scaling metrics');
      }
      // we do not want to floodProtect scaling event
      let tags = defaultTags;
      _.forEach(line, function (value, key) {
        if (value !== true && parseInt(value)) {
            statsd.gauge(prefix + 'heroku.dyno.' + key, parseInt(value), tags);
        }
      });
    }

    // Default
    else {
      if (DEBUG) {
        console.log('No match for line');
      }
    }
  }

  /**
   * Create properties obj for node-statsd from an statsd url
   * @param {string} [url]
   * @return {string|undefined}
   */
  function parseStatsdUrl(url) {
    if (url !== undefined) {
      url = urlUtil.parse(url);
      return {
        host: url.hostname,
        port: url.port
      };
    }

    return undefined; // Explicit is better than implicit :)
  }

  /**
   * Transform an object to an array of statsd tags
   * @param {object} tags
   * @return {string[]}
   */
  function tagsToArr (tags) {
    return _.transform(tags, function (arr, value, key) { return arr.push(key + ':' + value); }, []);
  }

  /**
   * Check if object contains list of keys
   */
  function hasKeys (object, keys) {
    return _.every(keys, _.partial(_.has, object));
  }

  /**
   * Construct allowed apps object from the environment vars containing
   * names, passwords and default tags for apps that may use the drain
   */
  function loadAllowedAppsFromEnv () {
    assert(process.env.ALLOWED_APPS, 'Environment variable ALLOWED_APPS required');
    let appNames = process.env.ALLOWED_APPS.split(',');
    let apps = appNames.map(function (name) {
      // Password
      var passwordEnvName = name.toUpperCase() + '_PASSWORD';
      var password = process.env[passwordEnvName];
      assert(password, 'Environment variable ' + passwordEnvName + ' required');

      // Tags
      var tags = process.env[name.toUpperCase() + '_TAGS'];
      tags = tags === undefined ? [] : tags.split(',');
      tags.push('app:' + name);

      // Prefix
      var prefix = process.env[name.toUpperCase() + '_PREFIX'] || '';
      if (prefix && prefix.slice(-1) !== '.') {
        prefix += '.';
      }

      return [name, { password, tags, prefix }];
    });

    return _.object(apps);
  }

  /**
   *
   */
  function extractNumber (string) {
    if (typeof string === 'string') {
      var match = string.match(DIGIT_REG_EXP);
      if (match !== null && match.length > 0) {
        return Number(match[0]);
      }
    }
    return undefined;
  }

  /**
   * Wrap a function with another function
   * @param {function} fn
   * @param {function} wrapper
   * @return {function}
   */
  function wrap (fn, wrapper) {
    return function (...args) {
      wrapper(...args);
      fn.apply(null, args);
    };
  }
}
