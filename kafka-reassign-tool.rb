#!/usr/bin/ruby

require 'yaml'
require 'json'
require 'open3'
require 'tempfile'
require 'optparse'

DEFAULT_KAFKA_ROOT='/usr/share/varadhi-kafka'
DEFAULT_KAFKA_CONFIG='/config/server.properties'

$kafka_root = nil
$zookeeper_url = nil
$input_assignment = nil
$throttle = nil
$retry_after = nil
$debug_log = false

def run(command, args = [], opts = {})

  opts = {
      :raise_on_exitstatus => true,
      :raise_on_err => true,
      :input => ''
  }.merge(opts)

  out = nil
  err = nil
  exit_status = nil
  Open3::popen3(command, *args) do |stdin, stdout, stderr, wait_thr|
    stdin << opts[:input]
    stdin.close
    out = stdout.read
    err = stderr.read
    exit_status = wait_thr.value
  end

  if (opts[:raise_on_exitstatus] && exit_status.exitstatus != 0) || (opts[:raise_on_err] && !err.empty?) then
    puts "#{command} failed. exitstatus=#{exit_status.exitstatus}, stderr:"
    puts err
    raise "#{command} failed"
  end

  {:out => out, :err => err, :exit_status => exit_status}
end

def get_kafka_root()
  return $kafka_root if $kafka_root
  DEFAULT_KAFKA_ROOT
end

def get_zk_url()
  return $zookeeper_url if $zookeeper_url

  config_file = get_kafka_root + DEFAULT_KAFKA_CONFIG

  if File.file? config_file then
    puts "Reading #{config_file}"
    File.open(config_file, 'r') do |file|
      file.each_line do |line|
        if line =~ /^zookeeper.connect=(.*)/ then
          $zookeeper_url = $1.strip
        end
      end
    end
  else
    puts "Config file #{config_file} does not exist"
  end

  raise "No zookeeper URL given" unless $zookeeper_url

  $zookeeper_url
end

def create_assignment_json(topic, partition, brokers)
  data = {
      :partitions => [{
                          :topic => topic,
                          :partition => partition,
                          :replicas => brokers
                      }],
      :version => 1
  }

  assignment_file = Tempfile.new("#{topic}-#{partition}")
  assignment_file << data.to_json()
  assignment_file.close

  puts "temp file: #{assignment_file.path}. content: #{data.to_json}" if $debug_log

  assignment_file.path
end

def verify_assignment(assignment_file)
  result = run(get_kafka_root + '/bin/kafka-reassign-partitions.sh', [
      '--zookeeper', get_zk_url,
      '--reassignment-json-file', assignment_file,
      '--verify'
  ])

  reassignment_lines = result[:out].lines.collect(&:strip).select { |l| l.start_with? "Reassignment" }
  in_progress = reassignment_lines.select { |l| l.end_with? "is still in progress" }.length
  completed = reassignment_lines.select { |l| l.end_with? "completed successfully" }.length

  puts "verify output: #{result[:out]}in_progress: #{in_progress}. completed: #{completed}" if $debug_log

  {
      :in_progress => in_progress,
      :completed => completed,
      :out => result[:out],
      :err => result[:err]
  }
end

def begin_reassignment(assignment_file)
  result = run(get_kafka_root + '/bin/kafka-reassign-partitions.sh', [
      '--zookeeper', get_zk_url,
      '--reassignment-json-file', assignment_file,
      '--execute',
      '--throttle', $throttle
  ])

  msg = result[:out].lines.collect(&:strip).select { |l| l.to_s.include? "Successfully started reassignment of partitions" }
  started = msg.length == 1

  puts "begin output: #{result[:out]}started: #{started}" if $debug_log

  if !started then
    puts "couldn't find the started message when starting the reassignment."
    puts "tool output:\n#{result[:out]}"
    puts "\njson_file:\n#{File.read(assignment_file)}"
    raise 'couldnt start the reassignment'
  end
end

def reassign(topic, partition, brokers)
  # first verify that it has already been reassigned.
  file_path = create_assignment_json(topic, partition, brokers)

  progress = verify_assignment file_path

  if (progress[:completed] == 1) then
    puts "#{topic}-#{partition} completed successfully"
    return
  end

  if (progress[:in_progress] == 0) then
    puts "#{topic}-#{partition} starting reassignment of partitions"

    begin_reassignment file_path

    puts "#{topic}-#{partition} started reassignment of partitions"
    sleep 5
  end

  loop do
    progress = verify_assignment file_path
    if (progress[:completed] == 1) then
      puts "#{topic}-#{partition} completed successfully"
      return
    elsif (progress[:in_progress] == 1) then
      puts "#{topic}-#{partition} is still in progress. Retrying after #{$retry_after} seconds"
    else
      puts "#{topic}-#{partition} failed.\nout:\n#{progress[:out]}\nerr:\n#{progress[:err]}"
      return
    end

    sleep $retry_after
  end
end

optparse = OptionParser.new do |opts|
  opts.on("--kafka-home <dir>",
          "Root directory of the Kafka installation",
          "  (standard Kafka scripts must be under bin/ directory there)",
          "  Default: #{DEFAULT_KAFKA_ROOT}") { |v| $kafka_root = v }
  opts.on("--zookeeper <url>",
          "The connection string for the zookeeper connection",
          "  If not specified, and attempt to read it from Kafka config file is made") { |v| $zookeeper_url = v }
  opts.on("--input <file>",
          "File containing partition assignment") { |v| $input_assignment = JSON.parse(File.read(v)) }
  opts.on("--throttle <throttle>",
          "replication throttle in B/s") { |v| $throttle = v }
  opts.on("--retry-after <retry>",
          "retry duration in sec after which the tool should look for completion status again") { |v| $retry_after = v.to_i }
  opts.on("--debug",
          "for debug logs") { |_| $debug_log = true }
end

begin
  optparse.parse!
  raise OptionParser::MissingArgument.new('input') if $input_assignment.nil?
  raise OptionParser::MissingArgument.new('throttle') if $throttle.nil?
  raise OptionParser::MissingArgument.new('retry-after') if $retry_after.nil?
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
  exit -1
end


zk_url = get_zk_url()
puts "Using zookeeper URL: #{zk_url}"

$input_assignment.each do |assignment|
  topic = assignment["topic"]
  partition = assignment["partition"]
  brokers = assignment["to"]

  reassign(topic, partition, brokers)
end
