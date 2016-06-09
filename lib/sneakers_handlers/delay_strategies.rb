module SneakersHandlers
  module DelayStrategies
    EXPONENTIAL = -> x { (x + 1) ** 2 }
  end
end
