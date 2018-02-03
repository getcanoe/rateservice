#!/usr/bin/env node
/**
 * This is a service that pulls in rate information and republishes it
 * over MQTT. See conf file rateservice.conf.
 */

const fs = require('fs')
const mqtt = require('mqtt')
const extend = require('extend')   // To merge objects
const winston = require('winston') // Logging lib
const schedule = require('node-schedule') //Schedule Published Rates
const request = require('request') // HTTP Requests for APIs

const CONFIG_FILE = 'rateservice.conf'

// Default config that is extended (merged) with CONFIG_FILE
let config = {
  logging: {
    level: 'info'
  },
  debug: false,
  mqtt: {
    url: 'wss://getcanoe.io:1884/mqtt',
    options: {
      username: 'test',
      password: 'gurka'
    },
    rates: {
      topic: 'rates',
      opts: {
        qos: 2,
        retain: true
      }
    }
  }
}

// MQTT Client
let mqttClient = null

// Flag to indicate we have already subscribed to topics
let subscribed = false

// Read configuration
let configure = () => {
  // Read config file if exists
  if (fs.existsSync(CONFIG_FILE)) {
    try {
      let fileConfig = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'))
      extend(true, config, fileConfig)
    } catch (e) {
      winston.error('Failed to parse config file: ' + CONFIG_FILE + e.message)
      process.exit(1)
    }
  }
  winston.level = config.logging.level
}

// Connect to MQTT
let connectMQTT = () => {
  mqttClient = mqtt.connect(config.mqtt.url, config.mqtt.options)
  mqttClient.on('connect', () => {
    winston.debug('Connected to MQTT server')
    subscribe()
    startScheduler()
  })

  // Where all subscribed messages come in
  mqttClient.on('message', (topic, message) => {
    switch (topic) {
      case 'rateservicecontrol':
        return handleControl(message)
    }
    winston.error('No handler for topic %s', topic)
  })
}

let publishRates = (payload, callback) => {
  mqttClient.publish(config.mqtt.rates.topic, payload, config.mqtt.rates.opts, callback)
}

// Subscribe to control
let subscribe = () => {
  if (!subscribed) {
    mqttClient.subscribe('rateservicecontrol')
    subscribed = true
  }
}

let handleControl = (message) => {
  let control = JSON.parse(message)
  winston.debug('Ignored control message: ', JSON.stringify(control))
  // TODO handle control commands
}

// Want to notify before shutting down
let handleAppExit = (options, err) => {
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

let configureSignals = () => {
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

let startScheduler = () => {
  schedule.scheduleJob('0 * * * * *', () => {
    request('https://min-api.cryptocompare.com/data/price?fsym=XRB&tsyms=BTC,ETH', (error, response, btcPrice) => {
      btcPrice = JSON.parse(btcPrice)
      request('https://bitpay.com/api/rates', (error, response, btcConversions) => {
        btcConversions = JSON.parse(btcConversions)
        btcConversions.forEach(function(pair) {
          pair.rate = pair.rate * btcPrice.BTC
        })
        btcConversions.push({
          code:"ETH",
          name:"Ethereum",
          rate: btcPrice.ETH
        })
        let rates = btcConversions.reduce(function(map, obj) {
          map[obj.code] = {
            name: obj.name,
            rate: obj.rate
          }
          return map
        }, {})
        publishRates(rates, () => {
          winston.info("Published Rates")
        })
      })
    })
  })
}


let main = () => {
  winston.info('Started RateService')
  configure()
  configureSignals()
  connectMQTT()
}

main()
