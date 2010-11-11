require File.expand_path('../spec_helper', __FILE__)

describe 'Woodchuck' do
  describe 'basic CRUD' do
    before :each do
      @id = @db.put('name' => 'foo')
    end

    it 'should allow creation of documents' do
      @db.get(@id)['name'].should == 'foo'
    end

    it 'should allow deletions' do
      @db.delete(@id)
      @db.get(@id).should be_nil
    end

    it 'should allow updates' do
      @db.put('_id' => @id, 'name' => 'bar')
      @db.get(@id)['name'].should == 'bar'
    end
  end

end
