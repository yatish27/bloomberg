require 'mechanize'
require 'parallel'
require 'csv'
file2 = CSV.open("bloomberg_symbols.csv","r").to_a

Parallel.each(file2,:in_threads=>100) do |symbol|
  begin
    agent = Mechanize.new
    agent.max_history = 1
    url = "http://www.bloomberg.com/quote/#{symbol[0]}/profile"
    page = agent.get(url)
    puts url
    hash = { }
    hash[:name] = page.at('h4').text.strip
    nodes = page.search(".left_column/div")

    hash[:country] = nodes[-1].text.strip
    hash[:city],hash[:zip] = nodes[-2].text.split(',') if nodes[-2]
    hash[:zip] = hash[:zip].strip if hash[:zip]
    hash[:address1] = nodes[0..-3].map{|n|n.text}.join(',').strip
    page.search(".right_column/div").each do |node|
      case
      when node.text.match(/Phone:/)
        hash[:phone] = node.text.gsub(/Phone:/,"").strip
      when node.at("a")
        hash[:website] = node.at('a')['href'].strip
      end
    end


    page.search(".exchange_type/ul/li").each do |node|
      case
      when node.text.match(/Sector:/)
        hash[:sector] = node.text.gsub(/Sector:/,"").strip
      when node.text.match(/^Industry:/)
        hash[:industry] = node.text.gsub(/^Industry:/,"").strip
      end
    end

    hash[:desc] = page.at('#extended_profile').text.strip if page.at('#extended_profile')

    CSV.open('bloomberg_companies.csv','a+') do |file|
      file << %w(name address1 city zip country website phone sector industry desc).map{|t| hash[t.to_sym]}
    end

    contacts = []

    page.search(".executives_two_cols/tr/td").each do |node|
      if node.at('.name')
        h ={}
        h[:name] = node.at('.name').text.strip
        h[:title] = node.at('.title').text.strip
        contacts << h
      end
    end

    CSV.open('bloomberg_contacts.csv','a+') do |file|
      contacts.each do |h|
        file << [hash[:name],h[:name],h[:title]]
      end
    end

  rescue=>e
    p e
  end
end
