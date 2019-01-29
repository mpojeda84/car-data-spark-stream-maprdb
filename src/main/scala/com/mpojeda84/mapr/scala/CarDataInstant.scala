package com.mpojeda84.mapr.scala

case class CarDataInstant(
                           _id: String,
                           vin: String,
                           make: String,
                           model: String,
                           year: Int,

                           latitude: Double,
                           longitude: Double,
                           speed: Double,
                           instantFuelEconomy: Double,
                           totalFuelEconomy: Double,
                           fuelRate: Double,

                           rpm: Int,
                           odometer: Int,
                           checkEngineMessage: String

                         )
