import pytest
from traits.implementation import *


def test_print_mariadb_host(mariadb_host):
    print(f"Mariadb Host: {mariadb_host}")


@pytest.fixture(scope="session")
def mariadb_host(request):
    """
    Return the HOST parameter to connect to a MariaDB. By default returns localhost's IP
    """
    return request.config.getoption("--mysql-host") if request.config.getoption(
        "--mysql-host") is not None else "127.0.0.1"

def test_convert_traits_key_to_int():
    assert TraitsUtility.convert_traits_key_to_int(TraitsKey(123)) == 123
    assert TraitsUtility.convert_traits_key_to_int(TraitsKey("456")) == 456
    with pytest.raises(ValueError):
        TraitsUtility.convert_traits_key_to_int(None)

def test_add_valid_user(rdbms_admin_connection):
    traits_utility = TraitsUtility(rdbms_admin_connection, rdbms_admin_connection, None)
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    user_email = "test@test.at"
    user_details = {"password": "test_pass", "is_admin": False}

    traits.add_user(user_email, user_details)

    user = traits_utility.get_user_by_email(user_email)
    assert user is not None
    assert user["email"] == user_email
    assert user["password"] == user_details["password"]
    assert user["is_admin"] == user_details["is_admin"]

def test_add_invalid_user(rdbms_admin_connection):
    traits_utility = TraitsUtility(rdbms_admin_connection, rdbms_admin_connection, None)
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    user_email = "invalid_email"
    user_details = {"password": "test_pass", "is_admin": False}

    with pytest.raises(ValueError):
        traits.add_user(user_email, user_details)

    user = traits_utility.get_user_by_email(user_email)
    assert user is None

def test_delete_user(rdbms_admin_connection):
    traits_utility = TraitsUtility(rdbms_admin_connection, rdbms_admin_connection, None)
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    user_email = "test@test.at"
    user_details = {"password": "test_pass", "is_admin": False}

    traits.add_user(user_email, user_details)
    user = traits_utility.get_user_by_email(user_email)
    assert user is not None

    traits.delete_user(user_email)
    user = traits_utility.get_user_by_email(user_email)
    assert user is None

def test_add_train(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(1)
    new_train_key = traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    assert new_train_key == train_key

def test_update_train_details(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(2)
    traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    traits.update_train_details(train_key, 120, TrainStatus.DELAYED)
    status = traits.get_train_current_status(train_key)
    assert status == TrainStatus.DELAYED

def test_add_train_station(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    station_key = TraitsKey(1)
    station_details = {"name": "WestBanhof", "location": "Vienna"}
    traits.add_train_station(station_key, station_details)
    with neo4j_db.session() as session:
        result = session.run("MATCH (s:Station {id: $station_id}) RETURN s", station_id=station_key.to_int())
        assert result.single() is not None

def test_connect_train_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)

    traits.add_train_station(start_station, {"name": "Start Station", "location": "Location A"})
    traits.add_train_station(end_station, {"name": "End Station", "location": "Location B"})
    traits.connect_train_stations(start_station, end_station, 15)
    with neo4j_db.session() as session:
        result = session.run("""
            MATCH (start:Station {id: $start_id})-[:CONNECTED_TO]->(end:Station {id: $end_id})
            RETURN start, end
        """, start_id=start_station.to_int(), end_id=end_station.to_int())
        assert result.single() is not None

def test_add_schedule(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    traits.add_train(TraitsKey(1), 100, TrainStatus.OPERATIONAL)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)
    train_key = TraitsKey(1)
    traits.add_train_station(start_station, {"name": "Hauptbanhof", "location": "01 Bezirk"})
    traits.add_train_station(end_station, {"name": "Spitelau", "location": "20 Bezirk"})
    traits.connect_train_stations(start_station, end_station, 30)
    stops = [(start_station, 5), (end_station, 10)]
    traits.add_schedule(train_key, 8, 0, stops, 1, 1, 2023, 31, 12, 2023)
    schedules = traits.utility.get_all_schedules()
    assert any(s[1] == train_key.to_int() for s in schedules)

def test_buy_ticket(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(5)
    traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    traits.add_user("buyer@test.at", {"password": "test_pass", "is_admin": False})
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)
    traits.add_train_station(start_station, {"name": "Meidling", "location": "Bezirk 12"})
    traits.add_train_station(end_station, {"name": "Floridsdorf", "location": "Bezirk 21"})
    traits.connect_train_stations(start_station, end_station, 30)
    stops = [(start_station, 5), (end_station, 10)]
    traits.add_schedule(train_key, 8, 0, stops, 1, 1, 2023, 31, 12, 2023)
    connection = {"train_id": train_key.to_int(), "departure_date": "2023-01-01", "price": 50.00}
    traits.buy_ticket("buyer@test.at", connection)
    purchases = traits.get_purchase_history("buyer@test.at")
    assert len(purchases) == 1

def test_get_purchase_history_empty(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    traits.add_user("nopurchases@example.com", {"password": "testpass", "is_admin": False})
    history = traits.get_purchase_history("nopurchases@example.com")
    assert history == []

def test_search_connections(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    start_station = TraitsKey(1)
    mid_station = TraitsKey(2)
    end_station = TraitsKey(3)
    traits.add_train_station(start_station, {"name": "Westbahnof", "location": "10 Bezirk"})
    traits.add_train_station(mid_station, {"name": "Floridsdorf", "location": "21 Bezirk"})
    traits.add_train_station(end_station, {"name": "krems", "location": "Krems"})
    traits.connect_train_stations(start_station, mid_station, 30)
    traits.connect_train_stations(mid_station, end_station, 30)
    connections = traits.search_connections(start_station, end_station, sort_by=SortingCriteria.NUMBER_OF_TRAIN_CHANGES)
    assert len(connections) > 0

def test_search_connections_no_connection(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    start_station = TraitsKey(1)
    end_station = TraitsKey(3)
    traits.add_train_station(start_station, {"name": "Krems", "location": "Krems"})
    traits.add_train_station(end_station, {"name": "Heiligenstadt", "location": "Bezirk 19"})
    connections = traits.search_connections(start_station, end_station)
    assert len(connections) == 0

def test_add_train_station_duplicate(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    station_key = TraitsKey(1)
    station_details = {"name": "WestBanhof", "location": "Vienna"}
    traits.add_train_station(station_key, station_details)
    with pytest.raises(ValueError):
        traits.add_train_station(station_key, station_details)

def test_connect_train_stations_invalid_time(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)

    traits.add_train_station(start_station, {"name": "Krems", "location": "Krems"})
    traits.add_train_station(end_station, {"name": "Tulln", "location": "Tulln"})
    with pytest.raises(ValueError):
        traits.connect_train_stations(start_station, end_station, -15)

def test_add_schedule_invalid_stops(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(1)
    traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)
    traits.add_train_station(start_station, {"name": "Hauptbanhof", "location": "01 Bezirk"})
    traits.add_train_station(end_station, {"name": "Spitelau", "location": "20 Bezirk"})
    traits.connect_train_stations(start_station, end_station, 30)
    stops = [(start_station, 5)]
    with pytest.raises(ValueError):
        traits.add_schedule(train_key, 8, 0, stops, 1, 1, 2023, 31, 12, 2023)


def test_add_schedule_unconnected_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(1)
    traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)
    traits.add_train_station(start_station, {"name": "Hauptbanhof", "location": "01 Bezirk"})
    traits.add_train_station(end_station, {"name": "Spitelau", "location": "20 Bezirk"})
    stops = [(start_station, 5), (end_station, 10)]
    with pytest.raises(ValueError):
        traits.add_schedule(train_key, 8, 0, stops, 1, 1, 2023, 31, 12, 2023)


def test_add_schedule_invalid_dates(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    train_key = TraitsKey(1)
    traits.add_train(train_key, 100, TrainStatus.OPERATIONAL)
    start_station = TraitsKey(1)
    end_station = TraitsKey(2)
    traits.add_train_station(start_station, {"name": "Hauptbanhof", "location": "01 Bezirk"})
    traits.add_train_station(end_station, {"name": "Spitelau", "location": "20 Bezirk"})
    traits.connect_train_stations(start_station, end_station, 30)
    stops = [(start_station, 5), (end_station, 10)]
    with pytest.raises(ValueError):
        traits.add_schedule(train_key, 8, 0, stops, 1, 1, 2023, 31, 12, 2022)

def test_add_user_empty_email_password(rdbms_admin_connection):
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    user_email = ""
    user_details = {"password": "", "is_admin": False}
    with pytest.raises(ValueError):
        traits.add_user(user_email, user_details)

def test_add_duplicate_user(rdbms_admin_connection):
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    user_email = "duplicate@test.com"
    user_details = {"password": "test_pass", "is_admin": False}
    traits.add_user(user_email, user_details)
    with pytest.raises(ValueError):
        traits.add_user(user_email, user_details)

def test_add_train_invalid_capacity(rdbms_admin_connection):
    traits = Traits(rdbms_admin_connection, rdbms_admin_connection, None)
    train_key = TraitsKey(1)
    invalid_capacities = [-10, 0]

    for capacity in invalid_capacities:
        with pytest.raises(ValueError):
            traits.add_train(train_key, capacity, TrainStatus.OPERATIONAL)

def test_add_station_duplicate_key(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    station_key = TraitsKey(3)
    station_details1 = {"name": "Hauptbahnof", "location": "Hauptbahnof"}
    station_details2 = {"name": "Tullnerfeld", "location": "Tullnerfeld"}
    traits.add_train_station(station_key, station_details1)
    with pytest.raises(ValueError):
        traits.add_train_station(station_key, station_details2)

def test_buy_ticket_invalid_user_or_train(rdbms_connection, rdbms_admin_connection, neo4j_db):
    traits = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    with pytest.raises(ValueError):
        traits.buy_ticket("invalid_user@test.com", {"train_id": 10, "departure_date": "29.06.2024", "price": 25.00})
    traits.add_user("buyer@test.com", {"password": "test_pass", "is_admin": False})
    with pytest.raises(ValueError):
        traits.buy_ticket("buyer@test.com", {"train_id": 10, "departure_date": "29.06.2024", "price": 25.00})

def test_get_all_users_empty(rdbms_connection, rdbms_admin_connection):
    utility = TraitsUtility(rdbms_connection, rdbms_admin_connection, None)
    users = utility.get_all_users()
    assert users == []

def test_get_all_schedules_empty(rdbms_connection, rdbms_admin_connection):
    utility = TraitsUtility(rdbms_connection, rdbms_admin_connection, None)
    schedules = utility.get_all_schedules()
    assert schedules == []

def test_get_all_trains_empty(rdbms_connection, rdbms_admin_connection):
    utility = TraitsUtility(rdbms_connection, rdbms_admin_connection, None)
    trains = utility.get_all_trains()
    assert trains == []

def test_get_user_by_email_none(rdbms_connection, rdbms_admin_connection):
    utility = TraitsUtility(rdbms_connection, rdbms_admin_connection, None)
    user = utility.get_user_by_email("nonexistent@example.com")
    assert user is None

def test_add_user_duplicate(rdbms_connection, rdbms_admin_connection):
    traits = Traits(rdbms_connection, rdbms_admin_connection, None)
    traits.add_user("duplicate@example.com", {"password": "test_pass", "is_admin": False})
    with pytest.raises(ValueError):
        traits.add_user("duplicate@example.com", {"password": "test_pass", "is_admin": False})
