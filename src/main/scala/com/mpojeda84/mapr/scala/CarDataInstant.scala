package com.mpojeda84.mapr.scala

case class CarDataInstant(
                           _id: String,

                           vin: String,
                           make: String,
                           year: String,
                           nhrTimeStamp: String,
                           hrTimeStamp: String,
                           latitude: String,
                           longitude: String,
                           speed: String,
                           instantFuelEconomy: String,
                           totalFuelEconomy: String,
                           fuelRate: String,
                           engineCoolant: String,
                           rpm: String,
                           altitude: String,
                           throttle: String,
                           timeSinceEngineStart: String,
                           ambientAirTemperature: String
                         )
