require 'byebug'
require "chronic"
require "elasticsearch"
require "date"
require 'rest-client'
require 'json'
require 'csv'
require "fileutils"



csv_output = []

File.open("LOCATIONS.csv", 'r') do |file|
  csv = CSV.new(file, headers: true)

  while row = csv.shift
    csv_output << row.to_h
  end
end

es_client = Elasticsearch::Client.new host: "http://XXXXXX:9200"
es_bulk = []
csv_output.each_slice(60) do |entry|
  entry.each do |location|
    begin
      api_result = RestClient::Request.execute method: :get, url: "http://api.openweathermap.org/data/2.5/weather?lat=#{location["Latitude"].to_f}&lon=#{location["Longitude"].to_f}&appid=XXXXXXXXXX"

      next unless ((JSON.parse(api_result)["coord"]["lat"].to_f.between?(50, 58)) && (JSON.parse(api_result)["coord"]["lon"].to_f.between?(-8, 2)))


      result = {
        "date" => Time.now.strftime("%FT%TZ"),
        "location" => "#{location["Latitude"].to_f},#{location["Longitude"].to_f}",
        "weather" => "#{JSON.parse(api_result)["weather"][0]["description"]}",
        "wind_speed" => "#{JSON.parse(api_result)["wind"]["speed"]}"
      }

      debugger

      es_bulk << {
        index: {
          _index: "weather-#{Time.now.strftime('%Y.%m.%d')}",
          _type: "_doc",
          data:   result,
        },
      }
    rescue
      next
    end
  end
  sleep 60
end

puts "Enriched the results"

es_bulk.each_slice(300) do |slice|
  es_client.bulk body: slice
end

puts "Sent results to ES"
