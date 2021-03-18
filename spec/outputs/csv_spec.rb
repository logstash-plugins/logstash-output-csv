require "csv"
require "tempfile"
require "logstash/devutils/rspec/spec_helper"
require "insist"
require "logstash/outputs/csv"

describe LogStash::Outputs::CSV do


  describe "Write a single field to a csv file" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","bar"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => "foo"
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == "bar\n"
    end
  end

  describe "write multiple fields and lines to a csv file" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo", "bar", "baz", "quux"]
          count => 2
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 2
      insist {lines[0]} == "bar,quux\n"
      insist {lines[1]} == "bar,quux\n"
    end
  end

  describe "missing event fields are empty in csv" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","bar", "baz", "quux"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "not_there", "baz"]
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == "bar,,quux\n"
    end
  end

  describe "commas are quoted properly" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","one,two", "baz", "quux"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == "\"one,two\",quux\n"
    end
  end

  describe "new lines are quoted properly" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","one\ntwo", "baz", "quux"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0][0]} == "one\ntwo"
    end
  end

  describe "fields that are are objects are written as JSON" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          message => '{"foo":{"one":"two"},"baz": "quux"}'
          count => 1
        }
      }
      filter {
        json { source => "message"}
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0][0]} == '{"one":"two"}'
    end
  end

  describe "can address nested field using field reference syntax" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          message => '{"foo":{"one":"two"},"baz": "quux"}'
          count => 1
        }
      }
      filter {
        json { source => "message"}
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["[foo][one]", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0][0]} == "two"
      insist {lines[0][1]} == "quux"
    end
  end

  describe "missing nested field is blank" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          message => '{"foo":{"one":"two"},"baz": "quux"}'
          count => 1
        }
      }
      filter {
        json { source => "message"}
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["[foo][missing]", "baz"]
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == ",quux\n"
    end
  end

  describe "can choose field seperator" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          message => '{"foo":"one","bar": "two"}'
          count => 1
        }
      }
      filter {
        json { source => "message"}
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "bar"]
          csv_options => {"col_sep" => "|"}
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == "one|two\n"
    end
  end
  describe "can choose line seperator" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          message => '{"foo":"one","bar": "two"}'
          count => 2
        }
      }
      filter {
        json { source => "message"}
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "bar"]
          csv_options => {"col_sep" => "|" "row_sep" => "\t"}
        }
      }
    CONFIG

    agent do
      lines = File.readlines(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0]} == "one|two\tone|two\t"
    end
  end

  describe "can escape rogue values" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","1+1", "baz", "=1+1"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0][0]} == "1+1"
      insist {lines[0][1]} == "'=1+1"
    end
  end

  describe "can turn off escaping rogue values" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo","1+1", "baz", "=1+1"]
          count => 1
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          spreadsheet_safe => false
          fields => ["foo", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 1
      insist {lines[0][0]} == "1+1"
      insist {lines[0][1]} == "=1+1"
    end
  end

  describe "can prepend field headers to output" do
    tmpfile = Tempfile.new('logstash-spec-output-csv')
    config <<-CONFIG
      input {
        generator {
          add_field => ["foo", "bar", "baz", "quux"]
          count => 3
        }
      }
      output {
        csv {
          path => "#{tmpfile.path}"
          write_headers => true
          fields => ["foo", "not_there", "baz"]
        }
      }
    CONFIG

    agent do
      lines = CSV.read(tmpfile.path)
      insist {lines.count} == 4
      insist {lines[0]} == ["foo","not_there","baz"]
      insist {lines[1]} == ["bar", nil, "quux"]
      insist {lines[2]} == ["bar", nil, "quux"]
      insist {lines[3]} == ["bar", nil, "quux"]
    end
  end

end
