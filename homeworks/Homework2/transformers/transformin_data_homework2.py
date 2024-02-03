import snakecase

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(column_name):
    res = [column_name[0].lower()]
    for char in column_name[1:]:
        if char.islower():
            res.append(char.lower())
        elif char.isupper():
            index_char = column_name.find(char)
            res.extend(['_', column_name[index_char:].lower()])
            break
        else :
            res.append(char)
    return ''.join(res)
@transformer
def transform(data, *args, **kwargs):

    print("preprocessing data with 0 paggengers ", data['passenger_count'].isin([0]).sum() , 'or trip_distance equal 0', data['trip_distance'].isin([0]).sum())
    

# Rename columns using the camel_to_snake function
    data = data.rename(columns=lambda x: camel_to_snake(x))
    data['lpep_pickup_date']=data['lpep_pickup_datetime'].dt.year
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

@test

def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert ((output['passenger_count'] > 0) & (output['trip_distance'] > 0)).any(), 'The output is undefined'
    #assert  output['trip_distance'].isin([0]).sum() == 0  , 'The output is undefined'
   