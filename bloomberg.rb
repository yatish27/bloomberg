require 'mechanize'
require 'parallel'
require 'csv'
COUNTRIES ="USA
Germany
India
Canada
Japan
Australia
Austria
Belgium
Brazil
Chile
China
Colombia
Denmark
Finland
France
Greece
Hong Kong
Indonesia
Ireland
Israel
Italy
Malaysia
Mexico
Netherlands
Nigeria
Norway
Philippines
Russia
Saudi Arabia
Singapore
South Africa
South Korea
Spain
Sweden
Switzerland
Taiwan
Thailand
Turkey
UAE
UK".split("\n")

Parallel.each(COUNTRIES,:in_threads=>10) do |country|

  agent = Mechanize.new
  agent.max_history = 1
  page_no = 1
  url = "http://www.bloomberg.com/markets/companies/country/#{country.downcase.gsub(" ","-")}/#{page_no}/"
  page = agent.get(url)

  while page.search("tr/.symbol").first
    begin
      CSV.open('bloomberg_symbols.csv','a+') do |file|
        page.search("tr/.symbol").each do |sym|
          file << [sym.text]
        end
      end
      puts url
      url = "http://www.bloomberg.com/markets/companies/country/#{country.downcase.gsub(" ","-")}/#{page_no}/"
      page = agent.get(url)
      page_no+=1
    rescue=>e
      p e
    end
  end
end
