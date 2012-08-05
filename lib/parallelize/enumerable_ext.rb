module Enumerable
  module Parallelize
    def self.it enum, num_threads, method, collect_exceptions, &block
      raise ArgumentError.new("Block not given") unless block_given?
      raise ArgumentError.new("Invalid number of threads") if num_threads < 1

      threads = []

      reap = lambda do |tidx|
        threads[tidx..-1].each do |t|
          t.raise Interrupt if t.alive?
          begin
            t.join
          rescue Exception
            nil
          end
        end
      end

      begin
        prev_trap = trap('INT') { Thread.current.raise Interrupt }
        enum.each_slice((enum.count{true} / num_threads.to_f).ceil) do |slice|
          threads << 
            case block.arity
            when 2
              Thread.new(slice, threads.length) { |my_slice, thread_idx|
                my_slice.send(method) { |e| yield e, thread_idx }
              }
            when 1
              Thread.new(slice) { |my_slice|
                my_slice.send(method) { |e| yield e }
              }
            when 0, -1
              raise ArgumentError.new("Invalid arity: #{block.arity}") if
                RUBY_VERSION !~ /^1.8\./ && block.arity == -1
              Thread.new(slice) { |my_slice|
                my_slice.send(method) { yield }
              }
            else
              raise ArgumentError.new("Invalid arity: #{block.arity}")
            end
        end

        exceptions = {}
        threads.each_with_index do |thr, idx|
          begin
            thr.join
          rescue Exception => e
            if collect_exceptions
              exceptions[idx] = e
              nil
            else
              reap.call idx + 1
              raise e
            end
          end
        end
      rescue Interrupt
        # Interrupts threads
        reap.call 0
        raise
      ensure
        trap('INT', prev_trap) if prev_trap
      end

      unless exceptions.empty?
        raise ParallelException.new(exceptions)
      end

      if method == :each
        threads
      elsif method == :map
        threads.map(&:value).inject(:+)
      end
    end
  end

  # Divides the Enumerable objects into pieces and execute with multiple threads
  # @return [Array] Threads.
  # @param [Fixnum] num_threads Number of concurrent threads
  # @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
  def peach num_threads, collect_exceptions = false, &block
    Parallelize.it self, num_threads, :each, collect_exceptions, &block
  end

  # Parallelized map.
  # @return [Array] Map function output for each element
  # @param [Fixnum] num_threads Number of concurrent threads
  # @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
  def pmap num_threads, collect_exceptions = false, &block
    Parallelize.it self, num_threads, :map, collect_exceptions, &block
  end
end

