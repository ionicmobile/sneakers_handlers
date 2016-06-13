module SneakersHandlers
  module DelayStrategies
    EXPONENTIAL = -> x { x ** 2 }
  end
end
