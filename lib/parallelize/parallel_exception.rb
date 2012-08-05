class ParallelException < Exception
  # @return [Hash] Hash of exceptions thrown. Indexed by thread index.
  attr_reader :exceptions

  def initialize(exceptions)
    @exceptions = exceptions
  end

  def to_s
    "Exceptions thrown during parallel execution: [#{@exceptions.inspect}]"
  end
end


