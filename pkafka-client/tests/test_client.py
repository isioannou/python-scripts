from kafka_client.producer import Key, Value
import datetime

def test_transform_to_dict():
    key_obj = Key(Name="John", Code="EU")
    value_obj = Value(Description="Some Description",
                                Name="Some Name",
                                StartTime=datetime.datetime.now(tz=datetime.timezone.utc))
    assert key_obj.to_dict() == {'Name': 'John', 'Code': 'EU'}
    assert value_obj.to_dict() == {'Description': 'Some Description', 'Name': 'Some Name', 'StartTime': value_obj.StartTime}