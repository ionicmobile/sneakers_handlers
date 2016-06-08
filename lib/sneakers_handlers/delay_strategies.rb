module SneakersHandlers
  module DelayStrategies
    EXPONENTIAL = lambda { |x| (x + 1) ** 2 }
  end
end
