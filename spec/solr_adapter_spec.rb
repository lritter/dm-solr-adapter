require 'spec'
require 'pathname'
require Pathname(__FILE__).dirname.parent.expand_path + 'lib/solr_adapter'

DataMapper.setup(:default, Addressable::URI.parse("solr://localhost:8983/solr"))

class Desk
  include DataMapper::Resource
  
  property :id, String, :key => true
  property :content, String, :field => :content_t
  property :width, Integer, :field => :width_i
  property :created_at, DateTime, :field => :created_at_dt
  # property :created_on, Date, :field => :created_on_dt
  
  before :create do
    self.created_at = DateTime.now
    # self.created_on = Date.today
  end
end


describe DataMapper::Adapters::SolrAdapter do
  def delete_all_desks
    DataMapper::Repository.adapters[:default].send(:with_connection) do |c|
      c.delete_by_query("+type:desk")
    end
  end
  
  def commit
    DataMapper::Repository.adapters[:default].send(:with_connection) do |c|
      c.commit
    end
  end

  before :all do
    delete_all_desks
  end
  
  # it "should mark new records as such" do
  #   Desk.new(:id => "don't save me", :content => "this is a test").new_record?.should be(true)
  # end
  # 
  # it "should properly create records" do
  #   successful = Desk.new(:id => "save me", :content => "this is a test").save
  #   successful.should be(true)
  # end
  # 
  # it "should mark saved records as not new" do
  #   desk = Desk.new(:id => "mark_me_as_not_new", :content => "this is a test")
  #   desk.save
  #   desk.new_record?.should be(false)
  # end
  # 
  # it "should allow a find with a single id" do
  #   Desk.new(:id => 2, :content => "this is a test").save
  #   desk = Desk.get(2)
  #   desk.class.should eql(Desk)
  #   desk.id.should eql(2.to_s)
  #   desk.content.should eql("this is a test")
  # end
  # 
  # it "should destroy a record" do
  #   Desk.new(:id => "to_be_destroyed", :content => "this is a test").save
  #   desk = Desk.get!("to_be_destroyed")
  #   desk.destroy.should be(true)
  # end
  # 
  # it "should properly update records" do
  # 
  #   desk = Desk.new(:id => "to_be_updated", :content => "this is a test", :width => 5)
  #   desk.save
  #   desk = Desk.get("to_be_updated")
  #   desk.content = "this is updated"
  #   desk.save.should be(true)
  #   desk2 = Desk.get("to_be_updated")
  #   desk2.content.should eql("this is updated")
  #   desk2.width.should eql(5)
  # end
  # 
  # it "should fetch all records in response to Model.all" do
  #   delete_all_desks
  # 
  #   num_to_create = 5
  #   num_to_create.times { |i| Desk.new(:id => i, :content => "I am #{i}").save}
  # 
  #   all_desks = Desk.all
  #   all_desks.size.should eql(num_to_create)
  #   desk_map = all_desks.inject({}) { |memo, desk| memo[desk.id] = desk; memo } ### don't know the order i'll get these back, so build a hash
  #   num_to_create.times do |i| 
  #     desk = desk_map[i.to_s]
  #     desk.id.should eql(i.to_s)
  #     desk.content.should eql("I am #{i}")
  #   end
  # end
  # 
  # it "should properly convert integers on retrieval" do
  #   desk = Desk.new(:id => "integer_conversion", :width => 5)
  #   desk.width.class.should eql(Fixnum)
  #   desk.save
  #   desk2 = Desk.get("integer_conversion")
  #   desk2.width.class.should eql(Fixnum)
  # end
  # 
  # it "should get records by eql matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   Desk.all(:width => 5).size.should eql(1)
  #   Desk.all(:content => "stuff").size.should eql(1)
  # end
  # 
  # it "should get records by not matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   Desk.all(:width.not => 5).size.should eql(2)
  # end
  # 
  # it "should get records by gt matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   all = Desk.all(:width.gt => 5)
  #   all.size.should eql(2)
  #   all.each { |d| d.width.should > 5}
  # end
  # 
  # it "should get records by gte matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   all = Desk.all(:width.gte => 5)
  #   all.size.should eql(3)
  #   all.each { |d| d.width.should >= 5}
  # end
  # 
  # it "should get records by lt matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   all = Desk.all(:width.lt => 54)
  #   all.size.should eql(2)
  #   all.each { |d| d.width.should < 54}
  # end
  # 
  # it "should get records by lte matcher" do
  #   delete_all_desks
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "has_digit_5", :width => 54).save
  #   Desk.new(:id => "not_eql_5", :width => 6).save
  #   all = Desk.all(:width.lte => 54)
  #   all.size.should eql(3)
  #   all.each { |d| d.width.should <= 54}
  # end
  # 
  # it "should get records by the like matcher" do 
  #   delete_all_desks
  #   Desk.new(:id => "peep", :content => "little bo peep fell of his sheep").save
  #   Desk.new(:id => "jack", :content => "jack and jill went up the hill").save
  # 
  #   all = Desk.all(:content.like => "j%")
  #   all.size.should == 1
  #   all.first.content.should =~ /j/
  # end
  # 
  # it "should order records" do
  #   delete_all_desks
  #   Desk.new(:id => "has_digit_5", :width => 54, :content => "junk").save
  #   Desk.new(:id => "eql_5", :width => 5, :content => "stuff").save
  #   Desk.new(:id => "also_eql_54", :width => 54, :content => 'alpha').save
  #   
  #   desks = Desk.all(:order => [:width])
  #   desks[0].width.should == 5
  # 
  #   desks = Desk.all(:order => [:width.desc])
  #   desks[0].width.should == 54
  # end
  
  it "should handle DateTime" do
    delete_all_desks
    desk = Desk.new(:id => 'dt_id')
    desk.save
    time = desk.created_at
    got_time = Desk.get!(desk.id).created_at
    got = Desk.get!(desk.id).created_at.strftime.gsub(got_time.zone,'')
    got.should eql(time.strftime.gsub(time.zone,''))
  end
  
end