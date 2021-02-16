require "./spec_helper"

describe Marten::DB::Query::Set do
  describe "#[]" do
    it "returns the expected record for a given index when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1].should eq tag_2
    end

    it "returns the expected record for a given index when the query set already fetched the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[0].should eq tag_1
      qset[1].should eq tag_2
      qset[2].should eq tag_3
      qset[3].should eq tag_4
    end

    it "returns the expected records for a given range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1..3].to_a.should eq [tag_2, tag_3, tag_4]
    end

    it "returns the expected records for a given range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[1..3].should eq [tag_2, tag_3, tag_4]
    end

    it "returns the expected records for an exclusive range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1...3].to_a.should eq [tag_2, tag_3]
    end

    it "returns the expected records for an exclusive range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[1...3].should eq [tag_2, tag_3]
    end

    it "returns the expected records for a begin-less range when the query set didn't already fetch the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[..3].to_a.should eq [tag_1, tag_2, tag_3, tag_4]
    end

    it "returns the expected records for a begin-less range when the query set already fetched the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[..3].should eq [tag_1, tag_2, tag_3, tag_4]
    end

    it "returns the expected records for an end-less range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      tag_5 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[2..].to_a.should eq [tag_3, tag_4, tag_5]
    end

    it "returns the expected records for an end-less range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      tag_5 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[2..].to_a.should eq [tag_3, tag_4, tag_5]
    end

    it "raises if the specified index is negative" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[-1]
      end
    end

    it "raises if the specified range has a negative beginning" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[-1..10]
      end
    end

    it "raises if the specified range has a negative end" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[10..-1]
      end
    end

    it "raises IndexError the specified index is out of bound when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)

      expect_raises(IndexError) do
        Marten::DB::Query::Set(Tag).new.all[20]
      end
    end

    it "raises IndexError the specified index is out of bound when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)

      expect_raises(IndexError) do
        qset = Marten::DB::Query::Set(Tag).new.all
        qset.each { }
        qset[20]
      end
    end
  end

  describe "#[]?" do
    it "returns the expected record for a given index when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1]?.should eq tag_2
    end

    it "returns the expected record for a given index when the query set already fetched the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[0]?.should eq tag_1
      qset[1]?.should eq tag_2
      qset[2]?.should eq tag_3
      qset[3]?.should eq tag_4
    end

    it "returns the expected records for a given range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1..3]?.not_nil!.to_a.should eq [tag_2, tag_3, tag_4]
    end

    it "returns the expected records for a given range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[1..3]?.should eq [tag_2, tag_3, tag_4]
    end

    it "returns the expected records for a begin-less range when the query set didn't already fetch the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[..3]?.not_nil!.to_a.should eq [tag_1, tag_2, tag_3, tag_4]
    end

    it "returns the expected records for an exclusive range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[1...3]?.not_nil!.to_a.should eq [tag_2, tag_3]
    end

    it "returns the expected records for an exclusive range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[1...3]?.not_nil!.should eq [tag_2, tag_3]
    end

    it "returns the expected records for a begin-less range when the query set already fetched the records" do
      tag_1 = Tag.create!(name: "coding", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[..3]?.not_nil!.should eq [tag_1, tag_2, tag_3, tag_4]
    end

    it "returns the expected records for an end-less range when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      tag_5 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)

      qset[2..]?.not_nil!.to_a.should eq [tag_3, tag_4, tag_5]
    end

    it "returns the expected records for an end-less range when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "ruby", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)
      tag_5 = Tag.create!(name: "typing", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:id)
      qset.each { }

      qset[2..]?.not_nil!.to_a.should eq [tag_3, tag_4, tag_5]
    end

    it "raises if the specified index is negative" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[-1]?
      end
    end

    it "raises if the specified range has a negative beginning" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[-1..10]?
      end
    end

    it "raises if the specified range has a negative end" do
      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Negative indexes are not supported") do
        Marten::DB::Query::Set(Tag).new.order(:id)[10..-1]?
      end
    end

    it "returns nil if the specified index is out of bound when the query set didn't already fetch the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)

      Marten::DB::Query::Set(Tag).new.all[20]?.should be_nil
    end

    it "returns nil the specified index is out of bound when the query set already fetched the records" do
      Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "crystal", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.all
      qset.each { }

      qset[20]?.should be_nil
    end
  end

  describe "#all" do
    it "returns a clone of the current query set" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)

      qset_1 = Marten::DB::Query::Set(Tag).new
      qset_2 = Marten::DB::Query::Set(Tag).new.filter(name__startswith: "c")

      new_qset_1 = qset_1.all
      new_qset_1.to_a.should eq [tag_1, tag_2]
      new_qset_1.object_id.should_not eq qset_1.object_id

      new_qset_2 = qset_2.all
      new_qset_2.to_a.should eq [tag_2]
      new_qset_2.object_id.should_not eq qset_2.object_id
    end
  end

  describe "#count" do
    it "returns the expected number of record for an unfiltered query set" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.count.should eq 3
    end

    it "returns the expected number of record for a filtered query set" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c).count.should eq 2
      Marten::DB::Query::Set(Tag).new.filter(name__startswith: "r").count.should eq 1
      Marten::DB::Query::Set(Tag).new.filter(name__startswith: "x").count.should eq 0
    end
  end

  describe "#create" do
    it "returns the non-persisted model instance if it is invalid" do
      tag = Marten::DB::Query::Set(Tag).new.create(name: nil)
      tag.valid?.should be_false
      tag.persisted?.should be_false
    end

    it "returns the persisted model instance if it is valid" do
      tag = Marten::DB::Query::Set(Tag).new.create(name: "crystal", is_active: true)
      tag.valid?.should be_true
      tag.persisted?.should be_true
    end

    it "allows to initialize the new invalid object in a dedicated block" do
      tag = Marten::DB::Query::Set(Tag).new.create(is_active: nil) do |o|
        o.name = "ruby"
      end
      tag.name.should eq "ruby"
      tag.valid?.should be_false
      tag.persisted?.should be_false
    end

    it "allows to initialize the new valid object in a dedicated block" do
      tag = Marten::DB::Query::Set(Tag).new.create(is_active: true) do |o|
        o.name = "crystal"
      end
      tag.valid?.should be_true
      tag.persisted?.should be_true
    end

    it "properly uses the default connection as expected when no special connection is targetted" do
      tag_1 = Marten::DB::Query::Set(Tag).new.create(name: "crystal", is_active: true)

      tag_2 = Marten::DB::Query::Set(Tag).new.create(is_active: false) do |o|
        o.name = "ruby"
      end

      Marten::DB::Query::Set(Tag).new.to_a.should eq [tag_1, tag_2]
      Marten::DB::Query::Set(Tag).new.using(:other).to_a.should be_empty
    end

    it "properly uses the targetted connection as expected" do
      tag_1 = Marten::DB::Query::Set(Tag).new.using(:other).create(name: "crystal", is_active: true)

      tag_2 = Marten::DB::Query::Set(Tag).new.using(:other).create(is_active: false) do |o|
        o.name = "ruby"
      end

      Marten::DB::Query::Set(Tag).new.to_a.should be_empty
      Marten::DB::Query::Set(Tag).new.using(:other).to_a.should eq [tag_1, tag_2]
    end
  end

  describe "#create!" do
    it "raises InvalidRecord if the model instance is invalid" do
      expect_raises(Marten::DB::Errors::InvalidRecord) do
        Marten::DB::Query::Set(Tag).new.create!(name: nil)
      end
    end

    it "returns the persisted model instance if it is valid" do
      tag = Marten::DB::Query::Set(Tag).new.create!(name: "crystal", is_active: true)
      tag.valid?.should be_true
      tag.persisted?.should be_true
    end

    it "allows to initialize the new invalid object in a dedicated block" do
      expect_raises(Marten::DB::Errors::InvalidRecord) do
        Marten::DB::Query::Set(Tag).new.create!(is_active: nil) do |o|
          o.name = "ruby"
        end
      end
    end

    it "allows to initialize the new valid object in a dedicated block" do
      tag = Marten::DB::Query::Set(Tag).new.create!(is_active: true) do |o|
        o.name = "crystal"
      end
      tag.valid?.should be_true
      tag.persisted?.should be_true
    end

    it "properly uses the default connection as expected when no special connection is targetted" do
      tag_1 = Marten::DB::Query::Set(Tag).new.create!(name: "crystal", is_active: true)

      tag_2 = Marten::DB::Query::Set(Tag).new.create!(is_active: false) do |o|
        o.name = "ruby"
      end

      Marten::DB::Query::Set(Tag).new.to_a.should eq [tag_1, tag_2]
      Marten::DB::Query::Set(Tag).new.using(:other).to_a.should be_empty
    end

    it "properly uses the targetted connection as expected" do
      tag_1 = Marten::DB::Query::Set(Tag).new.using(:other).create!(name: "crystal", is_active: true)

      tag_2 = Marten::DB::Query::Set(Tag).new.using(:other).create!(is_active: false) do |o|
        o.name = "ruby"
      end

      Marten::DB::Query::Set(Tag).new.to_a.should be_empty
      Marten::DB::Query::Set(Tag).new.using(:other).to_a.should eq [tag_1, tag_2]
    end
  end

  describe "#delete" do
    it "allows to delete the records targetted by a specific query set" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c).delete.should eq 2

      Marten::DB::Query::Set(Tag).new.to_a.should eq [tag_1]
    end

    it "properly deletes records by respecting relationships" do
      user_1 = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      post_1 = Post.create!(author: user_1, title: "Post 1")
      post_2 = Post.create!(author: user_2, title: "Post 2")
      post_3 = Post.create!(author: user_1, title: "Post 3")

      ShowcasedPost.create!(post: post_1)
      showcased_post_2 = ShowcasedPost.create!(post: post_2)
      ShowcasedPost.create!(post: post_3)

      Marten::DB::Query::Set(TestUser).new.filter(id: user_1.id).delete.should eq 5

      TestUser.all.map(&.id).to_set.should eq [user_2.id].to_set
      Post.all.map(&.id).should eq [post_2.id]
      ShowcasedPost.all.map(&.id).should eq [showcased_post_2.id]
    end

    it "is able to perform raw deletions" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c).delete(raw: true).should eq 2

      Marten::DB::Query::Set(Tag).new.to_a.should eq [tag_1]
    end

    it "raises if the query set is sliced" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Delete with sliced queries is not supported") do
        Marten::DB::Query::Set(Tag).new[..1].as?(Marten::DB::Query::Set(Tag)).not_nil!.delete
      end
    end

    it "raises if the query set involves joins" do
      user = TestUser.create!(username: "jd3", email: "jd3@example.com", first_name: "John", last_name: "Doe")
      Post.create!(author: user, title: "Example post")

      expect_raises(Marten::DB::Errors::UnmetQuerySetCondition, "Delete with joins is not supported") do
        Marten::DB::Query::Set(Post).new.join(:author).delete
      end
    end
  end

  describe "#each" do
    it "allows to iterate over the records targetted by the query set if it wasn't already fetched" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)

      tags = [] of Tag

      Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c).each do |t|
        tags << t
      end

      tags.should eq [tag_2, tag_3]
    end

    it "allows to iterate over the records targetted by the query set if it was already fetched" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)

      tags = [] of Tag

      qset = Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c)
      qset.each { }

      qset.each do |t|
        tags << t
      end

      tags.should eq [tag_2, tag_3]
    end
  end

  describe "#exclude" do
    it "allows to exclude the records matching predicates expressed as keyword arguments" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.exclude(name__startswith: :r)

      qset.to_a.should eq [tag_2, tag_3, tag_4]
    end

    it "allows to exclude the records matching predicates expressed using a q expression" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.exclude { q(name__startswith: :r) | q(name: "programming") }

      qset.to_a.should eq [tag_2, tag_3]
    end

    it "allows to exclude the records matching predicates expressed using a query node object" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.exclude(Marten::DB::Query::Node.new(name__startswith: :r))

      qset.to_a.should eq [tag_2, tag_3, tag_4]
    end

    it "properly returns an empty query set if there are no other records" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset_1 = Marten::DB::Query::Set(Tag).new.exclude(name__startswith: :c)
      qset_2 = Marten::DB::Query::Set(Tag).new.exclude { q(name__startswith: :c) | q(name: "programming") }
      qset_3 = Marten::DB::Query::Set(Tag).new.exclude(Marten::DB::Query::Node.new(name__startswith: :c))

      qset_1.exists?.should be_false
      qset_1.to_a.should be_empty

      qset_2.exists?.should be_false
      qset_2.to_a.should be_empty

      qset_3.exists?.should be_false
      qset_3.to_a.should be_empty
    end
  end

  describe "#exists?" do
    it "returns true if the queryset matches existing records and if it wasn't already fetched" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset_1 = Marten::DB::Query::Set(Tag).new.all
      qset_2 = Marten::DB::Query::Set(Tag).new.filter(name: "crystal")

      qset_1.exists?.should be_true
      qset_2.exists?.should be_true
    end

    it "returns true if the queryset matches existing records and if it was already fetched" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset_1 = Marten::DB::Query::Set(Tag).new.all
      qset_1.each { }

      qset_2 = Marten::DB::Query::Set(Tag).new.filter(name: "crystal")
      qset_2.each { }

      qset_1.exists?.should be_true
      qset_2.exists?.should be_true
    end

    it "returns false if the queryset doesn't match existing records and if it wasn't already fetched" do
      qset_1 = Marten::DB::Query::Set(Tag).new.all
      qset_1.exists?.should be_false

      Tag.create!(name: "crystal", is_active: true)

      qset_2 = Marten::DB::Query::Set(Tag).new.filter(name: "ruby")
      qset_2.exists?.should be_false
    end

    it "returns false if the queryset doesn't match existing records and if it was already fetched" do
      qset_1 = Marten::DB::Query::Set(Tag).new.all
      qset_1.each { }
      qset_1.exists?.should be_false

      Tag.create!(name: "crystal", is_active: true)

      qset_2 = Marten::DB::Query::Set(Tag).new.filter(name: "ruby")
      qset_2.each { }
      qset_2.exists?.should be_false
    end
  end

  describe "#filter" do
    it "allows to filter the records matching predicates expressed as keyword arguments" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c)

      qset.to_a.should eq [tag_2, tag_3]
    end

    it "allows to filter the records matching predicates expressed using a q expression" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.filter { q(name__startswith: :r) | q(name: "programming") }

      qset.to_a.should eq [tag_1, tag_4]
    end

    it "allows to filter the records matching predicates expressed using a query node object" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.filter(Marten::DB::Query::Node.new(name__startswith: :c))

      qset.to_a.should eq [tag_2, tag_3]
    end

    it "works as expected if the queryset was already fetched" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "coding", is_active: true)
      tag_4 = Tag.create!(name: "programming", is_active: true)

      qset_1 = Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c)
      qset_1.each { }

      qset_2 = Marten::DB::Query::Set(Tag).new.filter { q(name__startswith: :r) | q(name: "programming") }
      qset_2.each { }

      qset_3 = Marten::DB::Query::Set(Tag).new.filter(Marten::DB::Query::Node.new(name__startswith: :c))
      qset_3.each { }

      qset_1.to_a.should eq [tag_2, tag_3]
      qset_2.to_a.should eq [tag_1, tag_4]
      qset_3.to_a.should eq [tag_2, tag_3]
    end
  end

  describe "#first" do
    it "returns the first result for an ordered queryset" do
      Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:name)

      qset.first.should eq tag_2
    end

    it "returns the first result ordered according to primary keys for an unordered queryset" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.first.should eq tag_1
    end

    it "returns nil if the queryset doesn't match any records" do
      qset = Marten::DB::Query::Set(Tag).new
      qset.first.should be_nil
    end
  end

  describe "#get" do
    it "returns the record corresponding to predicates expressed as keyword arguments" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get(name: "crystal").should eq tag
      tag_qset.get(name: "crystal", is_active: true).should eq tag

      user_qset.get(username: "jd1").should eq user
      user_qset.get(email: "jd1@example.com").should eq user
      user_qset.get(username: "jd1", first_name: "John").should eq user
    end

    it "returns the record corresponding to predicates expressed as q expressions" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get { q(name: "crystal") }.should eq tag
      tag_qset.get { q(name: "crystal") & q(is_active: true) }.should eq tag

      user_qset.get { q(username: "jd1") }.should eq user
      user_qset.get { q(email: "jd1@example.com") }.should eq user
      user_qset.get { q(username: "jd1") & q(first_name: "John") }.should eq user
    end

    it "returns the record corresponding to predicates expressed using query node objects" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get(Marten::DB::Query::Node.new(name: "crystal")).should eq tag
      tag_qset.get(Marten::DB::Query::Node.new(name: "crystal", is_active: true)).should eq tag

      user_qset.get(Marten::DB::Query::Node.new(username: "jd1")).should eq user
      user_qset.get(Marten::DB::Query::Node.new(email: "jd1@example.com")).should eq user
      user_qset.get(Marten::DB::Query::Node.new(username: "jd1", first_name: "John")).should eq user
    end

    it "returns nil if predicates expressed as keyword arguments does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get(name: "ruby").should be_nil
      tag_qset.get(name: "crystal", is_active: false).should be_nil

      user_qset.get(username: "foo").should be_nil
      user_qset.get(username: "jd1", first_name: "Foo").should be_nil
    end

    it "returns nil if predicates expressed as q expressions does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get { q(name: "ruby") }.should be_nil
      tag_qset.get { q(name: "crystal") & q(is_active: false) }.should be_nil

      user_qset.get { q(username: "foo") }.should be_nil
      user_qset.get { q(username: "jd1") & q(first_name: "Foo") }.should be_nil
    end

    it "returns nil if predicates expressed as query node objects does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get(Marten::DB::Query::Node.new(name: "ruby")).should be_nil
      tag_qset.get(Marten::DB::Query::Node.new(name: "crystal", is_active: false)).should be_nil

      user_qset.get(Marten::DB::Query::Node.new(username: "foo")).should be_nil
      user_qset.get(Marten::DB::Query::Node.new(username: "jd1", first_name: "Foo")).should be_nil
    end

    it "raises if multiple records are found when using predicates expressed as keyword arguments" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get(name__startswith: "c")
      end
    end

    it "raises if multiple records are found when using predicates expressed as q expressions" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get { q(name__startswith: "c") & q(is_active: true) }
      end
    end

    it "raises if multiple records are found when using predicates expressed as query node objects" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get(Marten::DB::Query::Node.new(name__startswith: "c"))
      end
    end
  end

  describe "#get!" do
    it "returns the record corresponding to predicates expressed as keyword arguments" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get!(name: "crystal").should eq tag
      tag_qset.get!(name: "crystal", is_active: true).should eq tag

      user_qset.get!(username: "jd1").should eq user
      user_qset.get!(email: "jd1@example.com").should eq user
      user_qset.get!(username: "jd1", first_name: "John").should eq user
    end

    it "returns the record corresponding to predicates expressed as q expressions" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get! { q(name: "crystal") }.should eq tag
      tag_qset.get! { q(name: "crystal") & q(is_active: true) }.should eq tag

      user_qset.get! { q(username: "jd1") }.should eq user
      user_qset.get! { q(email: "jd1@example.com") }.should eq user
      user_qset.get! { q(username: "jd1") & q(first_name: "John") }.should eq user
    end

    it "returns the record corresponding to predicates expressed using query node objects" do
      tag = Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      user = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      tag_qset.get!(Marten::DB::Query::Node.new(name: "crystal")).should eq tag
      tag_qset.get!(Marten::DB::Query::Node.new(name: "crystal", is_active: true)).should eq tag

      user_qset.get!(Marten::DB::Query::Node.new(username: "jd1")).should eq user
      user_qset.get!(Marten::DB::Query::Node.new(email: "jd1@example.com")).should eq user
      user_qset.get!(Marten::DB::Query::Node.new(username: "jd1", first_name: "John")).should eq user
    end

    it "raises if predicates expressed as keyword arguments does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      expect_raises(Tag::NotFound) { tag_qset.get!(name: "ruby") }
      expect_raises(Tag::NotFound) { tag_qset.get!(name: "crystal", is_active: false) }

      expect_raises(TestUser::NotFound) { user_qset.get!(username: "foo") }
      expect_raises(TestUser::NotFound) { user_qset.get!(username: "jd1", first_name: "Foo") }
    end

    it "raises if predicates expressed as q expressions does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      expect_raises(Tag::NotFound) { tag_qset.get! { q(name: "ruby") } }
      expect_raises(Tag::NotFound) { tag_qset.get! { q(name: "crystal") & q(is_active: false) } }

      expect_raises(TestUser::NotFound) { user_qset.get! { q(username: "foo") } }
      expect_raises(TestUser::NotFound) { user_qset.get! { q(username: "jd1") & q(first_name: "Foo") } }
    end

    it "raises if predicates expressed as query node objects does not match anything" do
      Tag.create!(name: "crystal", is_active: true)
      TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")

      tag_qset = Marten::DB::Query::Set(Tag).new
      user_qset = Marten::DB::Query::Set(TestUser).new

      expect_raises(Tag::NotFound) { tag_qset.get!(Marten::DB::Query::Node.new(name: "ruby")) }
      expect_raises(Tag::NotFound) { tag_qset.get!(Marten::DB::Query::Node.new(name: "crystal", is_active: false)) }

      expect_raises(TestUser::NotFound) { user_qset.get!(Marten::DB::Query::Node.new(username: "foo")) }
      expect_raises(TestUser::NotFound) do
        user_qset.get!(Marten::DB::Query::Node.new(username: "jd1", first_name: "Foo"))
      end
    end

    it "raises if multiple records are found when using predicates expressed as keyword arguments" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get!(name__startswith: "c")
      end
    end

    it "raises if multiple records are found when using predicates expressed as q expressions" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get! { q(name__startswith: "c") & q(is_active: true) }
      end
    end

    it "raises if multiple records are found when using predicates expressed as query node objects" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      expect_raises(Marten::DB::Errors::MultipleRecordsFound) do
        qset.get!(Marten::DB::Query::Node.new(name__startswith: "c"))
      end
    end
  end

  describe "#inspect" do
    it "produces the expected output for an empty queryset" do
      Marten::DB::Query::Set(Tag).new.inspect.should eq "<Marten::DB::Query::Set(Tag) []>"
    end

    it "produces the expected output for a queryset with a small number of records" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new
      qset.each { }

      qset.inspect.should eq(
        "<Marten::DB::Query::Set(Tag) [" \
        "#<Tag:0x#{qset[0].object_id.to_s(16)} id: 1, name: \"crystal\", is_active: true>, " \
        "#<Tag:0x#{qset[1].object_id.to_s(16)} id: 2, name: \"coding\", is_active: true>" \
        "]>"
      )
    end

    it "produces the expected output for a queryset with a large number of records" do
      30.times do |i|
        Tag.create!(name: "tag-#{i}", is_active: true)
      end

      qset = Marten::DB::Query::Set(Tag).new
      qset.inspect.ends_with?(", ...(remaining truncated)...]>").should be_true
    end
  end

  describe "#join" do
    it "allows to specify relations to join as strings" do
      user_1 = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      Post.create!(author: user_1, title: "Post 1")
      Post.create!(author: user_2, title: "Post 2")

      qset = Marten::DB::Query::Set(Post).new

      qset.join("author")

      qset[0].__set_spec_author.should eq user_1
      qset[1].__set_spec_author.should eq user_2
    end

    it "allows to specify relations to join as symbols" do
      user_1 = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      Post.create!(author: user_1, title: "Post 1")
      Post.create!(author: user_2, title: "Post 2")

      qset = Marten::DB::Query::Set(Post).new

      qset.join(:author)

      qset[0].__set_spec_author.should eq user_1
      qset[1].__set_spec_author.should eq user_2
    end

    it "allows to specify multiple relations to join" do
      user_1 = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      Post.create!(author: user_1, title: "Post 1")
      Post.create!(author: user_2, title: "Post 2", updated_by: user_1)

      qset = Marten::DB::Query::Set(Post).new

      qset.join(:author, :updated_by)

      qset[0].__set_spec_author.should eq user_1
      qset[0].__set_spec_updated_by.should be_nil

      qset[1].__set_spec_author.should eq user_2
      qset[1].__set_spec_updated_by.should eq user_1
    end

    it "returns the expected queryset if no relation names are passed as arguments" do
      user_1 = TestUser.create!(username: "jd1", email: "jd1@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "jd2", email: "jd2@example.com", first_name: "John", last_name: "Doe")

      post_1 = Post.create!(author: user_1, title: "Post 1")
      post_2 = Post.create!(author: user_2, title: "Post 2", updated_by: user_1)

      qset = Marten::DB::Query::Set(Post).new

      qset.join

      qset[0].should eq post_1
      qset[0].__set_spec_author.should be_nil
      qset[0].__set_spec_updated_by.should be_nil

      qset[1].should eq post_2
      qset[1].__set_spec_author.should be_nil
      qset[1].__set_spec_updated_by.should be_nil
    end
  end

  describe "#last" do
    it "returns the last result for an ordered queryset" do
      Tag.create!(name: "crystal", is_active: true)
      tag_2 = Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new.order(:name)

      qset.last.should eq tag_2
    end

    it "returns the last result ordered according to primary keys for an unordered queryset" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.last.should eq tag_3
    end

    it "returns nil if the queryset doesn't match any records" do
      qset = Marten::DB::Query::Set(Tag).new
      qset.last.should be_nil
    end
  end

  describe "#model" do
    it "returns the associated model" do
      Marten::DB::Query::Set(Tag).new.model.should eq Tag
      Marten::DB::Query::Set(Post).new.model.should eq Post
    end
  end

  describe "#order" do
    it "allows to order using a specific column specified as a string" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.order("name").to_a.should eq [tag_2, tag_3, tag_1]
      qset.order("-name").to_a.should eq [tag_1, tag_3, tag_2]
    end

    it "allows to order using a specific column specified as a symbol" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.order(:name).to_a.should eq [tag_2, tag_3, tag_1]
      qset.order(:"-name").to_a.should eq [tag_1, tag_3, tag_2]
    end

    it "allows to order using multiple columns" do
      user_1 = TestUser.create!(username: "abc", email: "abc@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "ghi", email: "ghi@example.com", first_name: "John", last_name: "Bar")
      user_3 = TestUser.create!(username: "def", email: "def@example.com", first_name: "Bob", last_name: "Abc")

      qset = Marten::DB::Query::Set(TestUser).new

      qset.order(:first_name, :last_name).to_a.should eq [user_3, user_2, user_1]
    end
  end

  describe "#reverse" do
    it "reverses the current order of the considered queryset" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.create!(name: "crystal", is_active: true)
      tag_3 = Tag.create!(name: "programming", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.order(:name).reverse.to_a.should eq [tag_1, tag_3, tag_2]
    end
  end

  describe "#size" do
    it "returns the expected number of record for an unfiltered query set" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.size.should eq 3
    end

    it "returns the expected number of record for a filtered query set" do
      Tag.create!(name: "ruby", is_active: true)
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      Marten::DB::Query::Set(Tag).new.filter(name__startswith: :c).size.should eq 2
      Marten::DB::Query::Set(Tag).new.filter(name__startswith: "r").size.should eq 1
      Marten::DB::Query::Set(Tag).new.filter(name__startswith: "x").size.should eq 0
    end
  end

  describe "#to_s" do
    it "produces the expected output for an empty queryset" do
      Marten::DB::Query::Set(Tag).new.to_s.should eq "<Marten::DB::Query::Set(Tag) []>"
    end

    it "produces the expected output for a queryset with a small number of records" do
      Tag.create!(name: "crystal", is_active: true)
      Tag.create!(name: "coding", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new
      qset.each { }

      qset.to_s.should eq(
        "<Marten::DB::Query::Set(Tag) [" \
        "#<Tag:0x#{qset[0].object_id.to_s(16)} id: 1, name: \"crystal\", is_active: true>, " \
        "#<Tag:0x#{qset[1].object_id.to_s(16)} id: 2, name: \"coding\", is_active: true>" \
        "]>"
      )
    end

    it "produces the expected output for a queryset with a large number of records" do
      30.times do |i|
        Tag.create!(name: "tag-#{i}", is_active: true)
      end

      qset = Marten::DB::Query::Set(Tag).new
      qset.to_s.ends_with?(", ...(remaining truncated)...]>").should be_true
    end
  end

  describe "#update" do
    it "allows to update the records matching a given queryset with values specified as keyword arguments" do
      user_1 = TestUser.create!(username: "abc", email: "abc@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "ghi", email: "ghi@example.com", first_name: "John", last_name: "Bar")
      user_3 = TestUser.create!(username: "def", email: "def@example.com", first_name: "Bob", last_name: "Abc")

      qset = Marten::DB::Query::Set(TestUser).new

      qset.filter(first_name: "John").update(last_name: "Updated", is_admin: true).should eq 2

      user_1.reload
      user_1.first_name.should eq "John"
      user_1.last_name.should eq "Updated"
      user_1.is_admin.should be_true

      user_2.reload
      user_2.first_name.should eq "John"
      user_2.last_name.should eq "Updated"
      user_2.is_admin.should be_true

      user_3.reload
      user_3.first_name.should eq "Bob"
      user_3.last_name.should eq "Abc"
      user_3.is_admin.should be_falsey
    end

    it "allows to update the records matching a given queryset with values specified as a hash" do
      user_1 = TestUser.create!(username: "abc", email: "abc@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "ghi", email: "ghi@example.com", first_name: "John", last_name: "Bar")
      user_3 = TestUser.create!(username: "def", email: "def@example.com", first_name: "Bob", last_name: "Abc")

      qset = Marten::DB::Query::Set(TestUser).new

      qset.filter(first_name: "John").update({"last_name" => "Updated", "is_admin" => true}).should eq 2

      user_1.reload
      user_1.first_name.should eq "John"
      user_1.last_name.should eq "Updated"
      user_1.is_admin.should be_true

      user_2.reload
      user_2.first_name.should eq "John"
      user_2.last_name.should eq "Updated"
      user_2.is_admin.should be_true

      user_3.reload
      user_3.first_name.should eq "Bob"
      user_3.last_name.should eq "Abc"
      user_3.is_admin.should be_falsey
    end

    it "allows to update the records matching a given queryset with values specified as a named tuple" do
      user_1 = TestUser.create!(username: "abc", email: "abc@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "ghi", email: "ghi@example.com", first_name: "John", last_name: "Bar")
      user_3 = TestUser.create!(username: "def", email: "def@example.com", first_name: "Bob", last_name: "Abc")

      qset = Marten::DB::Query::Set(TestUser).new

      qset.filter(first_name: "John").update({last_name: "Updated", is_admin: true}).should eq 2

      user_1.reload
      user_1.first_name.should eq "John"
      user_1.last_name.should eq "Updated"
      user_1.is_admin.should be_true

      user_2.reload
      user_2.first_name.should eq "John"
      user_2.last_name.should eq "Updated"
      user_2.is_admin.should be_true

      user_3.reload
      user_3.first_name.should eq "Bob"
      user_3.last_name.should eq "Abc"
      user_3.is_admin.should be_falsey
    end

    it "returns 0 if no rows were affected by the updated" do
      user_1 = TestUser.create!(username: "abc", email: "abc@example.com", first_name: "John", last_name: "Doe")
      user_2 = TestUser.create!(username: "ghi", email: "ghi@example.com", first_name: "John", last_name: "Bar")
      user_3 = TestUser.create!(username: "def", email: "def@example.com", first_name: "Bob", last_name: "Abc")

      qset = Marten::DB::Query::Set(TestUser).new

      qset.filter(first_name: "Unknown").update({last_name: "Updated", is_admin: true}).should eq 0

      user_1.reload
      user_1.first_name.should eq "John"
      user_1.last_name.should eq "Doe"
      user_1.is_admin.should be_falsey

      user_2.reload
      user_2.first_name.should eq "John"
      user_2.last_name.should eq "Bar"
      user_2.is_admin.should be_falsey

      user_3.reload
      user_3.first_name.should eq "Bob"
      user_3.last_name.should eq "Abc"
      user_3.is_admin.should be_falsey
    end
  end

  describe "#using" do
    it "allows to switch to another DB connection" do
      tag_1 = Tag.create!(name: "ruby", is_active: true)
      tag_2 = Tag.using(:other).create!(name: "coding", is_active: true)
      tag_3 = Tag.using(:other).create!(name: "crystal", is_active: true)

      qset = Marten::DB::Query::Set(Tag).new

      qset.to_a.should eq [tag_1]
      qset.using(:other).to_a.should eq [tag_2, tag_3]
    end
  end
end

class Post
  def __set_spec_author
    @author
  end

  def __set_spec_updated_by
    @updated_by
  end
end