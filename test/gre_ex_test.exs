defmodule GreExTest do
  use ExUnit.Case
  doctest GreEx

  test "creates an event" do
    event = GreEx.make([name: "test", value: 123])
    assert is_reference(event) or is_tuple(event)
  end

  test "checks if a field exists" do
    event = GreEx.make([name: "test", value: 123])
    
    assert GreEx.has(:name, event)
    assert GreEx.has(:value, event)
    refute GreEx.has(:missing, event)
  end

  test "fetches a field value" do
    event = GreEx.make([name: "test", value: 123])
    
    assert GreEx.fetch(:name, event) == "test"
    assert GreEx.fetch(:value, event) == 123
    
    assert_raise ArgumentError, fn ->
      GreEx.fetch(:missing, event)
    end
  end

  test "finds a field value" do
    event = GreEx.make([name: "test", value: 123])
    
    assert GreEx.find(:name, event) == {true, "test"}
    assert GreEx.find(:value, event) == {true, 123}
    assert GreEx.find(:missing, event) == false
  end

  test "gets all field names" do
    event = GreEx.make([name: "test", value: 123])
    
    keys = GreEx.keys(event)
    assert :name in keys
    assert :value in keys
    assert length(keys) == 2
  end

  test "gets all field pairs" do
    pairs = [name: "test", value: 123]
    event = GreEx.make(pairs)
    
    result = GreEx.pairs(event)
    assert Keyword.equal?(result, pairs)
  end

  test "appends a field" do
    event = GreEx.make([name: "test"])
    
    updated = GreEx.append({:value, 123}, event)
    assert GreEx.has(:name, updated)
    assert GreEx.has(:value, updated)
    assert GreEx.fetch(:value, updated) == 123
  end

  test "merges events" do
    event1 = GreEx.make([name: "test"])
    event2 = GreEx.make([value: 123])
    
    merged = GreEx.merge(event1, event2)
    assert GreEx.has(:name, merged)
    assert GreEx.has(:value, merged)
    assert GreEx.fetch(:name, merged) == "test"
    assert GreEx.fetch(:value, merged) == 123
  end
end
