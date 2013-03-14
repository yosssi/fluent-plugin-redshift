module Fluent


class RedshiftOutput < BufferedOutput
  Fluent::Plugin.register_output('redshift', self)

  def initialize
    super
    require 'aws-sdk'
    require 'zlib'
    require 'time'
    require 'tempfile'
    require 'pg'
    require 'json'
    require 'csv'
  end

  config_param :record_log_tag, :string, :default => 'log'
  # s3
  config_param :aws_key_id, :string
  config_param :aws_sec_key, :string
  config_param :s3_bucket, :string
  config_param :s3_endpoint, :string, :default => nil
  config_param :path, :string, :default => ""
  config_param :timestamp_key_format, :string, :default => 'year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M'
  config_param :utc, :bool, :default => false
  # redshift
  config_param :redshift_host, :string
  config_param :redshift_port, :integer, :default => 5439
  config_param :redshift_dbname, :string
  config_param :redshift_user, :string
  config_param :redshift_password, :string
  config_param :redshift_tablename, :string
  # file format
  config_param :file_type, :string, :default => nil  # json, tsv, csv
  config_param :delimiter, :string, :default => nil
  # for debug
  config_param :log_suffix, :string, :default => ''

  def configure(conf)
    super
    @path = "#{@path}/" if /.+[^\/]$/ =~ @path
    @path = "" if @path == "/"
    @utc = true if conf['utc']
    @db_conf = {
      host:@redshift_host,
      port:@redshift_port,
      dbname:@redshift_dbname,
      user:@redshift_user,
      password:@redshift_password
    }
    @delimiter = determine_delimiter(@file_type) if @delimiter.nil? or @delimiter.empty?
    $log.debug format_log("redshift file_type:#{@file_type} delimiter:'#{@delimiter}'")
    @copy_sql_template = "copy #{@redshift_tablename} from '%s' CREDENTIALS 'aws_access_key_id=#{@aws_key_id};aws_secret_access_key=%s' delimiter '#{@delimiter}' REMOVEQUOTES GZIP;"
  end

  def start
    super
    # init s3 conf
    options = {
      :access_key_id     => @aws_key_id,
      :secret_access_key => @aws_sec_key
    }
    options[:s3_endpoint] = @s3_endpoint if @s3_endpoint
    @s3 = AWS::S3.new(options)
    @bucket = @s3.buckets[@s3_bucket]
  end

  def format(tag, time, record)
    (json?) ? record.to_msgpack : "#{record[@record_log_tag]}\n"
  end

  def write(chunk)
    # create a gz file
    tmp = Tempfile.new("s3-")
    tmp = (json?) ? create_gz_file_from_json(tmp, chunk, @delimiter)
                  : create_gz_file_from_msgpack(tmp, chunk)

    # no data -> skip
    unless tmp
      $log.debug format_log("received no valid data. ")
      return false # for debug
    end

    # create a file path with time format
    s3path = create_s3path(@bucket, @path)

    # upload gz to s3
    @bucket.objects[s3path].write(Pathname.new(tmp.path),
                                  :acl => :bucket_owner_full_control)
    # copy gz on s3 to redshift
    s3_uri = "s3://#{@s3_bucket}/#{s3path}"
    sql = @copy_sql_template % [s3_uri, @aws_sec_key]
    $log.debug  format_log("start copying. s3_uri=#{s3_uri}")
    conn = nil
    begin
      conn = PG.connect(@db_conf)
      conn.exec(sql)
      $log.info format_log("completed copying to redshift. s3_uri=#{s3_uri}")
    rescue PG::Error => e
      $log.error format_log("failed to copy data into redshift. s3_uri=#{s3_uri}"), :error=>e.to_s
      raise e if e.result.nil? # retry if connection errors
    ensure
      conn.close rescue nil if conn
    end
    true # for debug
  end

  protected
  def format_log(message)
    "#{message} #{@log_suffix}" if @log_suffix and not @log_suffix.empty?
  end

  private
  def json?
    @file_type == 'json'
  end

  def create_gz_file_from_msgpack(dst_file, chunk)
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.write_to(gzw)
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def create_gz_file_from_json(dst_file, chunk, delimiter)
    # fetch the table definition from redshift
    redshift_table_columns = fetch_table_columns
    if redshift_table_columns == nil
      raise "failed to fetch the redshift table definition."
    elsif redshift_table_columns.empty?
      $log.warn format_log("no table on redshift. table_name=#{@redshift_tablename}")
      return nil
    end

    # convert json to tsv format text
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.msgpack_each do |record|
        begin
          tsv_text = json_to_table_text(redshift_table_columns, record[@record_log_tag], delimiter)
          gzw.write(tsv_text) if tsv_text and not tsv_text.empty?
        rescue => e
          $log.error format_log("failed to create table text from json. text=(#{record[@record_log_tag]})"), :error=>$!.to_s
          $log.error_backtrace
        end
      end
      return nil unless gzw.pos > 0
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def determine_delimiter(file_type)
    case file_type
    when 'json', 'tsv'
      "\t"
    when "csv"
      ','
    else
      raise Fluent::ConfigError, "Invalid file_type:#{file_type}."
    end
  end

  def fetch_table_columns
    fetch_columns_sql = "select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '#{@redshift_tablename}' order by ordinal_position;"
    conn = PG.connect(@db_conf)
    begin
      columns = nil
      conn.exec(fetch_columns_sql) do |result|
        columns = result.collect{|row| row['column_name']}
      end
      columns
    ensure
      conn.close rescue nil
    end
  end

  def json_to_table_text(redshift_table_columns, json_text, delimiter)
    return "" if json_text.nil? or json_text.empty?

    # parse json text
    json_obj = nil
    begin
      json_obj = JSON.parse(json_text)
    rescue => e
      $log.warn format_log("failed to parse json. "), :error=>e.to_s
      return ""
    end
    return "" unless json_obj

    # extract values from json
    val_list = redshift_table_columns.collect do |cn|
      val = json_obj[cn]
      val = nil unless val and not val.to_s.empty?
      val = JSON.generate(val) if val.kind_of?(Hash) or val.kind_of?(Array)
      val.to_s unless val.nil?
    end
    if val_list.all?{|v| v.nil? or v.empty?}
      $log.warn format_log("no data match for table columns on redshift. json_text=#{json_text} table_columns=#{redshift_table_columns}")
      return ""
    end

    # generate tsv text
    begin
      CSV.generate(:col_sep=>delimiter, :quote_char => '"') do |row|
        row << val_list # inlude new line
      end
    rescue => e
      $log.debug format_log("failed to generate csv val_list:#{val_list} delimiter:(#{delimiter})")
      raise e
    end
  end

  def create_s3path(bucket, path)
    timestamp_key = (@utc) ? Time.now.utc.strftime(@timestamp_key_format) : Time.now.strftime(@timestamp_key_format)
    i = 0
    begin
      suffix = "_#{'%02d' % i}"
      s3path = "#{path}#{timestamp_key}#{suffix}.gz"
      i += 1
    end while bucket.objects[s3path].exists?
    s3path
  end

end


end
