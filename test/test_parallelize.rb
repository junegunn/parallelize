require 'helper'
require 'thread'

class TestParallelize < Test::Unit::TestCase
	def test_parallelize
		num_threads = 4

		m = Mutex.new
		cnt = 0
		threads = parallelize(num_threads) do
			m.synchronize do
				cnt += 1
			end
		end

		assert_equal num_threads, threads.length
		assert_equal num_threads, cnt
		assert threads.all? { |t| t.is_a? Thread }
	end

	def test_parallelize_thread_idx
		num_threads = 4

		m = Mutex.new
		max_thread_idx = 0
		threads = parallelize(num_threads) do |thread_idx|
			m.synchronize do
				max_thread_idx = [thread_idx, max_thread_idx].max
			end
		end
		assert_equal num_threads - 1, max_thread_idx
		assert_equal num_threads, threads.length
		assert threads.all? { |t| t.is_a? Thread }
	end

	def test_parallelize_exception
		num_threads = 4
		delay = 3

		[true, false].each do |collect_exception|
			st = Time.now
			begin
				threads = parallelize(num_threads, collect_exception) do |thread_idx|
					unknown_method_should_fail if [0, 1].include? thread_idx
					sleep delay
				end
			rescue Exception => e
				ex = e
			end

			if collect_exception
				assert_equal ParallelException, ex.class
				assert_equal Hash,              ex.exceptions.class
				assert_equal 2,                 ex.exceptions.length
				assert_equal [0, 1],            ex.exceptions.keys
				assert Time.now - st >= delay, "Did not collect exceptions"
				p e.exceptions
			else
				assert_equal NameError, ex.class
				assert Time.now - st < delay, "Did not return immediately"
			end
		end
	end

	def test_peach
		count = 110
		num_threads = 4

		m = Mutex.new
		r = Hash.new { |h, k| h[k] = [] }
		range = (0...count)
		range.peach(num_threads) do |elem, thread_idx|
			m.synchronize { r[thread_idx] << elem }
		end

		assert_equal count, r.values.inject(0) { |sum, arr| sum + arr.length }
		assert_equal range.to_a.sort, r.values.inject([]) { |cc, arr| cc += arr; cc }.sort
	end

	def test_peach_exception
		count = 110
		num_threads = 4

		m = Mutex.new
		[true, false].each do |collect_exception|
			r = Hash.new { |h, k| h[k] = [] }
			range = (0...count)
			begin
				range.peach(num_threads, collect_exception) do |elem, thread_idx|
					unknown_method_should_fail if [0, 1].include? thread_idx
					m.synchronize { r[thread_idx] << elem }
				end
			rescue Exception => e
				ex = e
			end

			if collect_exception
				assert_equal ParallelException, ex.class
				assert_equal Hash,              ex.exceptions.class
				assert_equal 2,                 ex.exceptions.length
				assert_equal [0, 1],            ex.exceptions.keys
				assert r.values.inject(0) { |sum, arr| sum + arr.length } < count
				assert r.values.inject(0) { |sum, arr| sum + arr.length } > count / num_threads
				p e.exceptions
			else
				assert_equal NameError, ex.class
				assert r.values.inject(0) { |sum, arr| sum + arr.length } < count
			end
		end
	end

	def test_peach_invalid_arity_block
		assert_raise(ArgumentError) {
			(0..100).peach(4) do |a, b, c|

			end
		}
		assert_raise(ArgumentError) {
			(0..100).peach(4)
		}
		assert_raise(ArgumentError) {
			(0..100).peach(0) do |a|
			end
		}

		# Should be ok
		(0..100).peach(4) do
		end
	end

  def test_empty_peach
    [].peach(4) {}
  end

  def test_pmap
    thr = 8
    n_per_thr = (101.0 / 8).ceil
    i = -1
    assert_equal(
      (0..100).map { |e, idx| i += 1; "#{e} by #{i / n_per_thr}" },
      (0..100).pmap(thr) { |e, tidx| "#{e} by #{tidx}" }
    )
  end

  def test_reap
    ic = 0
    t = Thread.new {
      begin
        parallelize(2) do |idx|
          begin
            raise Exception.new if idx == 0
            loop {}
          rescue Interrupt
            puts "Interrupted (#{idx}-#{ic})"
            ic += 1
          end
        end
      rescue Interrupt
        puts "Interrupted (parallelize-#{ic})"
        ic += 1
      rescue Exception
        puts "Exception thrown from a child"
        raise
      end
    }

    assert_raise(Exception) { t.join }
    assert_equal 1, ic
  end

  def test_interrupt
    ic = 0
    t = Thread.new {
      begin
        parallelize(2) do |idx|
          begin
            loop {}
          rescue Interrupt
            puts "Interrupted (#{idx}-#{ic})"
            ic += 1
          end
        end
      rescue Interrupt
        puts "Interrupted (parallelize-#{ic})"
        ic += 1
      end
    }

    sleep 2
    t.raise Interrupt
    t.join
    assert_equal 3, ic
  end
end
