'use strict';

let throng = require('throng');
let WORKERS = parseInt(process.env.WEB_CONCURRENCY || 1, 10);
let port = parseInt(process.env.PORT || 3000, 10);
let redis = require('redis-url').connect(process.env.REDIS_URL);
let LIMIT_REQ = parseInt(process.env.LIMIT_REQ || 5, 10);
let EXPIRE_REQ = parseInt(process.env.EXPIRE_REQ || 10, 10);

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
  let logfmt = require('logfmt');
  let express = require('express');
  let through = require('through');
  let urlUtil = require('url');
  let StatsD = require('node-statsd');
  let basicAuth = require('basic-auth');
  let app = module.exports = express();
  let statsd = new StatsD(parseStatsdUrl(process.env.STATSD_URL));
  let allowedApps = loadAllowedAppsFromEnv();

  if (process.env.DEBUG) {
    console.log('Allowed apps', allowedApps);
    statsd.send = wrap(statsd.send.bind(statsd), console.log.bind(null, 'Intercepted: statsd.send(%s):'));
  }

  app.use(logfmt.bodyParserStream());
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
      if (process.env.DEBUG) {
        console.log('Unauthorized access by %s', auth.name);
      }
    }
  });
  app.use(function floodProtection(req, res, next) {
    if (process.env.FLOOD_PROTECTION === undefined) return next();
    var key = req.appName;
    if (process.env.DEBUG) {
      console.log('Protection for app: ', key);
    }
    if (key !== undefined) {
      redis.get(key, function (err, buffer) {
        if (err) {
          if (process.env.DEBUG) {
            console.error('Redis get error:', err);
          }
          return next();
        }
        if (buffer >= LIMIT_REQ) {
          req.floodProtectionOff = 1;
          redis.del(key, function (err) {
            if (process.env.DEBUG) {
              console.error('Redis del error:', err);
            }
          });
          return next();
        } else {
          redis.incr(key, function (err) {
            if (process.env.DEBUG) {
              console.error('Redis incr error:', err);
            }
            redis.expire(key, EXPIRE_REQ, function (err) {
              if (process.env.DEBUG) {
                console.error('Redis expire error:', err);
              }
            });
          });
          return next();
        }
      });
    }

  });

  app.post('/', function (req, res) {
    if (req.body !== undefined || process.env.FLOOD_PROTECTION !== undefined) {
      if (req.floodProtection !== 1) {
        req.body.pipe(through(line => processLine(line, req.prefix, req.defaultTags)));
      }
    }
    res.send('OK');
  });

  app.listen(port, function () {
    console.log('Server listening on port ' + port);
  });


  /**
   * Matches a line against a rule and processes it
   * @param {object} line
   */
  function processLine (line, prefix, defaultTags) {
    // Dyno metrics
    if (hasKeys(line, ['heroku', 'source', 'dyno'])) {
      if (process.env.DEBUG) {
        console.log('Processing dyno metrics');
      }
      let tags = tagsToArr({ dyno: line.source });
      tags = _.union(tags, defaultTags);
      let metrics = _.pick(line, (_, key) => key.startsWith('sample#'));
      _.forEach(metrics, function (value, key) {
        key = key.split('#')[1];
        key = key.replace(/_/g, '.');
        statsd.histogram(prefix + 'heroku.dyno.' + key, extractNumber(value), tags);
      });
    }

    // Router metrics
    else if (hasKeys(line, ['heroku', 'router', 'path', 'method', 'dyno', 'status', 'connect', 'service', 'at'])) {
      if (process.env.DEBUG) {
        console.log('Processing router metrics');
      }
      let tags = tagsToArr(_.pick(line, ['dyno', 'method', 'status', 'path', 'host', 'code', 'desc', 'at']));
      tags = _.union(tags, defaultTags);
      statsd.histogram(prefix + 'heroku.router.request.connect', extractNumber(line.connect), tags);
      statsd.histogram(prefix + 'heroku.router.request.service', extractNumber(line.service), tags);
      if (line.at === 'error') {
        statsd.increment(prefix + 'heroku.router.error', 1, tags);
      }
    }

    // Postgres metrics
    else if (hasKeys(line, ['source', 'heroku-postgres'])) {
      if (process.env.DEBUG) {
        console.log('Processing postgres metrics');
      }
      let tags = tagsToArr({ source: line.source });
      tags = _.union(tags, defaultTags);
      let metrics = _.pick(line, (_, key) => key.startsWith('sample#'));
      _.forEach(metrics, function (value, key) {
        key = key.split('#')[1];
        statsd.histogram(prefix + 'heroku.postgres.' + key, extractNumber(value), tags);
        // TODO: Use statsd counters or gauges for some postgres metrics (db size, table count, ..)
      });
    }

    // Scaling event
    else if (line.api === true && line.Scale === true) {
      if (process.env.DEBUG) {
        console.log('Processing scaling metrics');
      }
      let tags = defaultTags;
      _.forEach(line, function (value, key) {
        if (value !== true && parseInt(value)) {
            statsd.gauge(prefix + 'heroku.dyno.' + key, parseInt(value), tags);
        }
      });
    }

    // Default
    else {
      if (process.env.DEBUG) {
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
    return _.transform(tags, (arr, value, key) => arr.push(key + ':' + value), []);
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
      var match = string.match(/[\d\.]+/);
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
