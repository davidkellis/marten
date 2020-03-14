require "./spec_helper"

describe Marten::HTTP::Response::NotAllowed do
  describe "::new" do
    it "allows to initialize a 405 HTTP response by specifying allowed methods" do
      response = Marten::HTTP::Response::NotAllowed.new(%w(GET POST))
      response.status.should eq 405
      response.headers.should eq({ "Allow" => "GET, POST" })
    end
  end
end