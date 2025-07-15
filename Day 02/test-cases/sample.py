import pytest

@pytest.fixture
def sampleData():
    return [1, 2, 3,4]



def test_sum(sampleData):
    assert sum(sampleData) == 10

@pytest.fixture
def base():
    return 2

@pytest.mark.parametrize("power, expected",[(1,2),(3,8),(4,16)])
def test_exponentation(base,power, expected):
    assert base**power == expected