/**
 * This is a service that pulls in rate information and republishes it
 * over MQTT, rateservice.conf.
 */

const fs = require('fs')
const mqtt = require('mqtt')
const extend = require('extend')   // To merge objects
const winston = require('winston') // Logging lib

const CONFIG_FILE = 'rateservice.conf'

// Default config that is extended (merged) with CONFIG_FILE
var config = {
  logging: {
    level: 'info'
  },
  debug: false,
  mqtt: {
    url: 'wss://getcanoe.io',
    options: {
      username: 'test',
      password: 'gurka'
    },
    rates: {
      topic: 'rates',
      opts: {
        qos: 2,
        retain: false
      }
    }
  }
}

// MQTT Client
var mqttClient = null

// Flag to indicate we have already subscribed to topics
var subscribed = false

// Read configuration
function configure () {
  // Read config file if exists
  if (fs.existsSync(CONFIG_FILE)) {
    try {
      var fileConfig = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'))
      extend(true, config, fileConfig)
    } catch (e) {
      winston.error('Failed to parse config file: ' + CONFIG_FILE + e.message)
      process.exit(1)
    }
  }
  winston.level = config.logging.level
}

// Connect to MQTT
function connectMQTT () {
  mqttClient = mqtt.connect(config.mqtt.url, config.mqtt.options)
  mqttClient.on('connect', function () {
    winston.debug('CONNECTED TO MQTT')
    subscribe()
  })

  // Where all subscribed messages come in
  mqttClient.on('message', function (topic, message) {
    switch (topic) {
      case 'rateservicecontrol':
        return handleControl(message)
    }
    winston.error('No handler for topic %s', topic)
  })
}

function publishRates (payload, callback) {
  mqttClient.publish(config.mqtt.rates.topic, payload, config.mqtt.rates.opts, callback)
}

// Subscribe to control
function subscribe () {
  if (!subscribed) {
    mqttClient.subscribe('rateservicecontrol')
    subscribed = true
  }
}

function handleControl (message) {
  var control = JSON.parse(message)
  winston.debug('PARSED CONTROL: ', control)
  // TODO handle control commands
}

// Want to notify before shutting down
function handleAppExit (options, err) {
  if (err) {
    winston.error(err.stack)
  }
  if (options.cleanup) {
    winston.info('Cleaning up...')
    mqttClient.end(true)
  }
  if (options.exit) {
    winston.info('Calling exit...')
    process.exit()
  }
}

function configureSignals () {
  // Handle the different ways an application can shutdown
  process.on('exit', handleAppExit.bind(null, {
    cleanup: true
  }))
  process.on('SIGINT', handleAppExit.bind(null, {
    exit: true
  }))
  process.on('uncaughtException', handleAppExit.bind(null, {
    exit: true
  }))
}

function main () {
  configure()
  configureSignals()
  connectMQTT()
}

main()
