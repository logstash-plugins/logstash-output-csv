require "csv"
require "tempfile"
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/csv"

describe LogStash::Outputs::CSV do

  subject { described_class.new(options) }

  let(:tmpfile) { Tempfile.new('logstash-spec-output-csv').path }
  let(:output) { File.readlines(tmpfile) }
  let(:csv_output) { CSV.read(tmpfile) }

  before(:each) do
    subject.register
    subject.multi_receive(events)
  end

  context "when configured with a single field" do
    let(:events) { [ LogStash::Event.new("foo" => "bar") ] }
    let(:options) { { "path" => tmpfile, "fields" => "foo" } }
    it "writes a single field to a csv file" do
      expect(output.count).to eq(1)
      expect(output.first).to eq("bar\n")
    end
  end

  context "when receiving multiple events with multiple fields" do
    let(:events) do
      [ LogStash::Event.new("foo" => "bar", "baz" => "quux"),
        LogStash::Event.new("foo" => "bar", "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"] } }
    it "writes a line per event " do
      expect(output.count).to eq(2)
    end
    it "writes configured fields for each line" do
      expect(output[0]).to eq("bar,quux\n")
      expect(output[1]).to eq("bar,quux\n")
    end
  end

  context "with missing event fields" do
    let(:events) do
      [ LogStash::Event.new("foo" => "bar", "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "not_there", "baz"] } }

    it "skips on the resulting line" do
      expect(output.size).to eq(1)
      expect(output[0]).to eq("bar,,quux\n")
    end
  end

  context "when field values have commas" do
    let(:events) do
      [ LogStash::Event.new("foo" => "one,two", "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"] } }
    it "correctly escapes them" do
      expect(output.size).to eq(1)
      expect(output[0]).to eq("\"one,two\",quux\n")
    end
  end

  context "when fields contain special characters" do
    let(:events) do
      [ LogStash::Event.new("foo" => 'one\ntwo', "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"] } }
    it "correctly escapes them" do
      expect(csv_output.size).to eq(1)
      expect(csv_output[0]).to eq(['one\ntwo', 'quux'])
    end
  end

  context "fields that contain objects" do
    let(:events) do
      [ LogStash::Event.new("foo" => {"one" => "two"}, "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"] } }

    it "are written as json" do
      expect(csv_output.size).to eq(1)
      expect(csv_output[0][0]).to eq('{"one":"two"}')
    end
  end
  context "with address nested field" do
    let(:events) do
      [ LogStash::Event.new("foo" => {"one" => "two"}, "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["[foo][one]", "baz"] } }

    it "are referenced using field references" do
      expect(csv_output.size).to eq(1)
      expect(csv_output[0][0]).to eq('two')
      expect(csv_output[0][1]).to eq('quux')
    end
  end

  context "missing nested field" do
    let(:events) do
      [ LogStash::Event.new("foo" => {"one" => "two"}, "baz" => "quux") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["[foo][missing]", "baz"] } }

    it "are blank" do
      expect(output.size).to eq(1)
      expect(output[0]).to eq(",quux\n")
    end
  end

  describe "field separator" do
    let(:events) do
      [ LogStash::Event.new("foo" => "one", "baz" => "two") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"], "csv_options" => {"col_sep" => "|" } } }

    it "uses separator in output" do
      expect(output.size).to eq(1)
      expect(output[0]).to eq("one|two\n")
    end
  end

  describe "line seperator" do
    let(:events) do
      [ LogStash::Event.new("foo" => "one", "baz" => "two"),
        LogStash::Event.new("foo" => "one", "baz" => "two") ]
    end
    let(:options) { { "path" => tmpfile, "fields" => ["foo", "baz"], "csv_options" => {"col_sep" => "|", "row_sep" => "\t" } } }

    it "uses separator in output" do
      expect(output.size).to eq(1)
      expect(output[0]).to eq("one|two\tone|two\t")
    end
  end

  context "with rogue values" do
    let(:event_data) do
      {
        "f1" => "1+1",
        "f2" => "=1+1",
        "f3" => "+1+1",
        "f4" => "-1+1",
        "f5" => "@1+1"
      }
    end
    let(:events) do
      [ LogStash::Event.new(event_data) ]
    end

    let(:options) { { "path" => tmpfile, "fields" => ["f1", "f2", "f3", "f4", "f5"] } }
    it "escapes them correctly" do
      expect(csv_output.size).to eq(1)
      expect(csv_output[0][0]).to eq("1+1")
      expect(csv_output[0][1]).to eq("'=1+1")
      expect(csv_output[0][2]).to eq("'+1+1")
      expect(csv_output[0][3]).to eq("'-1+1")
      expect(csv_output[0][4]).to eq("'@1+1")
    end

    context "when escaping is turned off" do
      let(:options) { super().merge("spreadsheet_safe" => false) }
      it "doesn't escapes values" do
        expect(csv_output.size).to eq(1)
        expect(csv_output[0][0]).to eq("1+1")
        expect(csv_output[0][1]).to eq("=1+1")
      end
    end
  end
end
