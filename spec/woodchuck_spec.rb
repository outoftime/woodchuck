require File.expand_path('../spec_helper', __FILE__)

describe 'Woodchuck' do
  describe 'basic CRUD' do
    before :each do
      @id = @db.add('name' => 'foo')
    end

    it 'allows creation of documents' do
      @db.get(@id)['name'].should == 'foo'
    end

    it 'allows deletions' do
      @db.delete(@id)
      @db.get(@id).should be_nil
    end

    it 'allows updates' do
      @db.update(@id, 'name' => 'bar')
      @db.get(@id)['name'].should == 'bar'
    end
  end

  describe 'get all for map' do
    before :each do
      @ids = []
      @ids << @db.add('name' => 'foo', 'category' => 'pizza')
      @ids << @db.add('name' => 'bar', 'category' => 'ice cream')
      @db.map(:by_category, 'function(doc) { emit(doc.category, doc); }')
    end

    it 'returns all documents in lexical key order' do
      @db.all(:by_category).map { |doc| doc['name'] }.should == %w(bar foo)
    end

    it 'returns all documents with a limit' do
      @db.all(:by_category, :limit => 1).map do |doc|
        doc['name']
      end.should == %w(bar)
    end

    it 'returns all documents with an offset' do
      @db.all(:by_category, :offset => 1).map do |doc|
        doc['name']
      end.should == %w(foo)
    end

    it 'does not return documents deleted before materialization' do
      @db.delete(@ids.last)
      @db.all(:by_category).map do |doc|
        doc['name']
      end.should == %w(foo)
    end

    it 'does not return documents deleted after materialization' do
      @db.all(:by_category)
      @db.delete(@ids.last)
      @db.all(:by_category).map do |doc|
        doc['name']
      end.should == %w(foo)
    end

    it 'remaps when document updated after materialization' do
      @db.all(:by_category)
      @db.update('_id' => @ids.last, 'name' => 'bar', 'category' => 'sushi')
      @db.all(:by_category).map do |doc|
        doc['name']
      end.should == %w(foo bar)
    end
  end
end
