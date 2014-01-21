/*jshint node:true, laxcomma:true */

/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'graphite' in the backends
 * configuration array:
 *
 *   backends: ['graphite']
 *
 * This backend supports the following config options:
 *
 *   graphiteHost: Hostname of graphite server.
 *   graphitePort: Port to contact graphite server at.
 */

var dgram = require('dgram');

// this will be instantiated to the logger
var l;

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;
var flush_counts;

var maxUDPLength;

// prefix configuration
var globalPrefix;
var prefixPersecond;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;
var globalSuffix;

// set up namespaces
var legacyNamespace  = true;
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace    = [];

var graphiteStats = {};

var post_stats = function graphite_post_stats(statArray) {
  var last_flush = graphiteStats.last_flush || 0;
  var last_exception = graphiteStats.last_exception || 0;
  var flush_time = graphiteStats.flush_time || 0;
  var flush_length = graphiteStats.flush_length || 0;
  if (graphiteHost) {
    try {
      var graphite = dgram.createSocket('udp4');
      graphite.addListener('error', function(connectionException){
        if (debug) {
          l.log(connectionException);
        }
      });
      var ts = Math.round(new Date().getTime() / 1000);
      var ts_suffix = ' ' + ts;
      var namespace = globalNamespace.concat(prefixStats).join(".");
      statArray.push(namespace + '.graphiteStats.last_exception' + globalSuffix + last_exception + ts_suffix);
      statArray.push(namespace + '.graphiteStats.last_flush'     + globalSuffix + last_flush     + ts_suffix);
      statArray.push(namespace + '.graphiteStats.flush_time'     + globalSuffix + flush_time     + ts_suffix);
      statArray.push(namespace + '.graphiteStats.flush_length'   + globalSuffix + flush_length   + ts_suffix);

      var starttime = Date.now();

      var flush_length = 0;
      var statArray_length = statArray.length;
      var bufferArray = [];
      var bufferLength = 0;
      for (var i = 0; i < statArray_length; i++) {
        var stat = statArray[i];
        if (bufferLength + stat.length > maxUDPLength) {
          var message = new Buffer(bufferArray.join("\n") + "\n");
          graphite.send(message, 0, message.length, graphitePort, graphiteHost);
          flush_length += message.length;
          bufferArray.length = 0;
          bufferLength = 0;
        }
        bufferArray.push(stat);
        bufferLength += stat.length;
      }
      if (bufferLength > 0) {
        var message = new Buffer(bufferArray.join("\n") + "\n");
        graphite.send(message, 0, message.length, graphitePort, graphiteHost);
        flush_length += message.length;
      }

      graphiteStats.flush_time = (Date.now() - starttime);
      graphiteStats.flush_length = flush_length;
      graphiteStats.last_flush = Math.round(new Date().getTime() / 1000);
    } catch(e){
      if (debug) {
        l.log(e);
      }
      graphiteStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
};

var flush_stats = function graphite_flush(ts, metrics) {
  var ts_suffix = ' ' + ts;
  var starttime = Date.now();
  var statArray = [];
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    var namespace = counterNamespace.concat(key);
    var value = counters[key];
    var valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

    if (legacyNamespace === true) {
      statArray.push(namespace.join(".")   + globalSuffix + valuePerSecond + ts_suffix);
      if (flush_counts) {
        statArray.push('stats_counts.' + key + globalSuffix + value + ts_suffix);
      }
    } else {
      statArray.push(namespace.concat('rate').join(".")  + globalSuffix + valuePerSecond + ts_suffix);
      if (flush_counts) {
        statArray.push(namespace.concat('count').join(".") + globalSuffix + value + ts_suffix);
      }
    }

    numStats += 1;
  }

  for (key in timer_data) {
    var namespace = timerNamespace.concat(key);
    var the_key = namespace.join(".");
    for (timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        statArray.push(the_key + '.' + timer_data_key + globalSuffix + timer_data[key][timer_data_key] + ts_suffix);
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          statArray.push(the_key + '.' + timer_data_key + '.' + timer_data_sub_key + globalSuffix +
                          timer_data[key][timer_data_key][timer_data_sub_key] + ts_suffix);
        }
      }
    }
    numStats += 1;
  }

  for (key in gauges) {
    var namespace = gaugesNamespace.concat(key);
    statArray.push(namespace.join(".") + globalSuffix + gauges[key] + ts_suffix);
    numStats += 1;
  }

  for (key in sets) {
    var namespace = setsNamespace.concat(key);
    statArray.push(namespace.join(".") + '.count' + globalSuffix + sets[key].values().length + ts_suffix);
    numStats += 1;
  }

  var namespace = globalNamespace.concat(prefixStats);
  if (legacyNamespace === true) {
    statArray.push(prefixStats + '.numStats' + globalSuffix + numStats + ts_suffix);
    statArray.push('stats.' + prefixStats + '.graphiteStats.calculationtime' + globalSuffix + (Date.now() - starttime) + ts_suffix);
    for (key in statsd_metrics) {
      statArray.push('stats.' + prefixStats + '.' + key + globalSuffix + statsd_metrics[key] + ts_suffix);
    }
  } else {
    statArray.push(namespace.join(".") + '.numStats' + globalSuffix + numStats + ts_suffix);
    statArray.push(namespace.join(".") + '.graphiteStats.calculationtime' + globalSuffix + (Date.now() - starttime) + ts_suffix);
    for (key in statsd_metrics) {
      var the_key = namespace.concat(key);
      statArray.push(the_key.join(".") + globalSuffix + statsd_metrics[key] + ts_suffix);
    }
  }
  post_stats(statArray);

  if (debug) {
   l.log("numStats: " + numStats);
  }
};

var backend_status = function graphite_status(writeCb) {
  for (var stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

exports.init = function graphite_init(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;
  config.graphite = config.graphite || {};
  globalPrefix    = config.graphite.globalPrefix;
  prefixCounter   = config.graphite.prefixCounter;
  prefixTimer     = config.graphite.prefixTimer;
  prefixGauge     = config.graphite.prefixGauge;
  prefixSet       = config.graphite.prefixSet;
  globalSuffix    = config.graphite.globalSuffix;
  legacyNamespace = config.graphite.legacyNamespace;

  // set defaults for prefixes & suffix
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;

  // In order to unconditionally add this string, it either needs to be
  // a single space if it was unset, OR surrounded by a . and a space if
  // it was set.
  globalSuffix  = globalSuffix !== undefined ? '.' + globalSuffix + ' ' : ' ';

  if (legacyNamespace === false) {
    if (globalPrefix !== "") {
      globalNamespace.push(globalPrefix);
      counterNamespace.push(globalPrefix);
      timerNamespace.push(globalPrefix);
      gaugesNamespace.push(globalPrefix);
      setsNamespace.push(globalPrefix);
    }

    if (prefixCounter !== "") {
      counterNamespace.push(prefixCounter);
    }
    if (prefixTimer !== "") {
      timerNamespace.push(prefixTimer);
    }
    if (prefixGauge !== "") {
      gaugesNamespace.push(prefixGauge);
    }
    if (prefixSet !== "") {
      setsNamespace.push(prefixSet);
    }
  } else {
      globalNamespace = ['stats'];
      counterNamespace = ['stats'];
      timerNamespace = ['stats', 'timers'];
      gaugesNamespace = ['stats', 'gauges'];
      setsNamespace = ['stats', 'sets'];
  }

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = startup_time;
  graphiteStats.flush_time = 0;
  graphiteStats.flush_length = 0;

  flushInterval = config.flushInterval;

  flush_counts = typeof(config.flush_counts) === "undefined" ? true : config.flush_counts;

  maxUDPLength = typeof(config.maxUDPLength) === "undefined" ? 508 : config.maxUDPLength;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
