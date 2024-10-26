from random import random

from traits.interface import TraitsInterface, TraitsUtilityInterface, TraitsKey, TrainStatus, SortingCriteria

# Import all the necessary default configurations
from traits.interface import BASE_USER_NAME, BASE_USER_PASS, ADMIN_USER_NAME, ADMIN_USER_PASS

from typing import List, Tuple, Optional, Dict
import re


# Implement the utility class. Add any additional method that you need
class TraitsUtility(TraitsUtilityInterface):

    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.rdbms_connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver

    @staticmethod
    def generate_sql_initialization_code() -> List[str]:
        return [
            "DROP DATABASE IF EXISTS test;",
            "CREATE DATABASE test;",
            f"DROP USER IF EXISTS '{ADMIN_USER_NAME}'@'%';",
            f"DROP USER IF EXISTS '{BASE_USER_NAME}'@'%';",
            f"CREATE USER '{ADMIN_USER_NAME}'@'%' IDENTIFIED BY '{ADMIN_USER_PASS}';",
            f"CREATE USER '{BASE_USER_NAME}'@'%' IDENTIFIED BY '{BASE_USER_PASS}';",
            f"GRANT ALL PRIVILEGES ON test.* TO '{ADMIN_USER_NAME}'@'%';",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON test.* TO '{BASE_USER_NAME}'@'%';",
            "FLUSH PRIVILEGES;",
            "USE test;",
            '''CREATE TABLE IF NOT EXISTS users (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                password VARCHAR(255), 
                is_admin BOOLEAN DEFAULT FALSE
            );''',
            '''CREATE TABLE IF NOT EXISTS trains(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                status ENUM('OPERATIONAL', 'DELAYED', 'BROKEN') DEFAULT 'OPERATIONAL',
                capacity INT NOT NULL
            );''',
            '''CREATE TABLE IF NOT EXISTS train_stations(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                location VARCHAR(255) NOT NULL
            );''',
            '''CREATE TABLE IF NOT EXISTS schedules(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                train_id INT NOT NULL,
                start_train_station_id INT NOT NULL,
                end_train_station_id INT NOT NULL,
                departure_time TIME NOT NULL,
                departure_date DATE NOT NULL,
                arrival_time TIME NOT NULL,
                arrival_date DATE NOT NULL,
                FOREIGN KEY (train_id) REFERENCES trains(id),
                FOREIGN KEY (start_train_station_id) REFERENCES train_stations(id),
                FOREIGN KEY (end_train_station_id) REFERENCES train_stations(id),
                CHECK (departure_date <= arrival_date)
            );''',
            '''CREATE TABLE IF NOT EXISTS tickets(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                schedule_id INT NOT NULL,
                purchase_date DATETIME NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (schedule_id) REFERENCES schedules(id)
            );''',
            '''CREATE TABLE IF NOT EXISTS seat_reservations(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                ticket_id INT NOT NULL,
                number_of_seats INT NOT NULL,
                FOREIGN KEY (ticket_id) REFERENCES tickets(id),
                CHECK (number_of_seats > 0)
            );''',
            '''CREATE TABLE IF NOT EXISTS connections(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                start_station_id INT NOT NULL,
                end_station_id INT NOT NULL,
                travel_time_minutes INT NOT NULL,
                FOREIGN KEY (start_station_id) REFERENCES train_stations(id),
                FOREIGN KEY (end_station_id) REFERENCES train_stations(id),
                CHECK (travel_time_minutes > 0)
            );''',
            '''CREATE TABLE IF NOT EXISTS schedule_stops(
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                schedule_id INT NOT NULL,
                station_id INT NOT NULL,
                stop_order INT NOT NULL,
                waiting_time INT NOT NULL,
                FOREIGN KEY (schedule_id) REFERENCES schedules(id),
                FOREIGN KEY (station_id) REFERENCES train_stations(id)
            );''',
        ]

    def get_all_users(self) -> List[TraitsKey]:
        """
        Return all the users stored in the database
        """
        sql_query = "SELECT * FROM users"
        try:
            cur = self.rdbms_admin_connection.cursor()
            cur.execute(sql_query)
            users = cur.fetchall()
            cur.close()
            return list(users)
        except Exception as e:
            print(f"An error occurred during getting all users: {e}")

    def get_all_schedules(self) -> List[TraitsKey]:
        """
        Return all the schedules stored in the database
        """
        sql_query = "SELECT * FROM schedules"
        try:
            cur = self.rdbms_admin_connection.cursor()
            cur.execute(sql_query)
            schedules = cur.fetchall()
            cur.close()
            return schedules
        except Exception as e:
            print(f"An error occurred during getting all schedules: {e}")

    def get_all_trains(self) -> List[TraitsKey]:
        """
        Return all the trains stored in the database
        """
        sql_query = "SELECT * FROM trains"
        try:
            cur = self.rdbms_admin_connection.cursor()
            cur.execute(sql_query)
            trains = cur.fetchall()
            cur.close()
            return trains
        except Exception as e:
            print(f"An error occurred during getting all trains: {e}")

    def get_user_by_email(self, user_email: str) -> Optional[Dict]:
        """
        Get the user details by email
        """
        sql_query = f"SELECT * FROM users WHERE email = '{user_email}'"
        try:
            cur = self.rdbms_admin_connection.cursor()
            cur.execute(sql_query)
            user = cur.fetchone()
            cur.close()
            if user:
                return {"id": user[0], "email": user[1], "password": user[2], "is_admin": user[3]}
            else:
                return None
        except Exception as e:
            print(f"An error occurred during getting a user by email: {e}")

    @staticmethod
    def convert_traits_key_to_int(traits_key: Optional[TraitsKey]) -> int:
        if traits_key is None:
            raise ValueError("TraitsKey cannot be None")
        if isinstance(traits_key.id, str):
            try:
                return int(traits_key.id)
            except ValueError:
                raise ValueError("TraitsKey value is a string but cannot be converted to int")
        elif isinstance(traits_key.id, int):
            return traits_key.id
        else:
            raise ValueError("TraitsKey value is neither a string nor an int")


# Implement the main class that you need to implement
class Traits(TraitsInterface):

    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.rdbms_connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver
        self.utility = TraitsUtility(rdbms_connection, rdbms_admin_connection, neo4j_driver)

    ########################################################################
    # Basic Features
    ########################################################################

    def search_connections(self, starting_station_key: TraitsKey, ending_station_key: TraitsKey,
                           travel_time_day: int = None, travel_time_month: int = None, travel_time_year: int = None,
                           is_departure_time=True,
                           sort_by: SortingCriteria = SortingCriteria.OVERALL_TRAVEL_TIME, is_ascending: bool = True,
                           limit: int = 5) -> List:
        """
        Search Train Connections (between two stations).
        Sorting criteria can be one of the following: overall travel time, number of train changes, waiting time, and estimated price

        Return the connections from a starting and ending stations, possibly including changes at interchanging stations.
        Returns an empty list if no connections are possible
        Raise a ValueError in case of errors and if the starting or ending stations are the same
        """

        if starting_station_key == ending_station_key:
            raise ValueError("Starting and ending stations are the same")

        if starting_station_key is None or ending_station_key is None:
            raise ValueError("Starting and ending stations cannot be None")

        # Check if the stations exist
        neo_query = """
            MATCH (s:Station {id: $station_id})
            RETURN s
        """
        for station_key in [starting_station_key, ending_station_key]:
            with self.neo4j_driver.session() as session:
                result = session.run(neo_query, station_id=station_key.to_int())
                if result.single() is None:
                    raise ValueError(f"Station with key {station_key.to_int()} does not exist in the database")

        sort_order = "ASC" if is_ascending else "DESC"
        sort_f = {
            SortingCriteria.OVERALL_TRAVEL_TIME: "travel_time",
            SortingCriteria.NUMBER_OF_TRAIN_CHANGES: "train_changes",
            SortingCriteria.OVERALL_WAITING_TIME: "waiting_time",
            SortingCriteria.ESTIMATED_PRICE: "estimated_price"
        }.get(sort_by, "travel_time")

        # travel time constraints for the query
        travel_time_constraints = ""
        travel_time = None
        if travel_time_day is not None and travel_time_month is not None and travel_time_year is not None:
            travel_time = f"{travel_time_year}-{travel_time_month:02d}-{travel_time_day:02d}"
            if is_departure_time:
                travel_time_constraints = "AND all(r in relationships(path) WHERE r.departure_time >= $travel_time)"
            else:
                travel_time_constraints = "AND all(r in relationships(path) WHERE r.arrival_time <= $travel_time)"

        neo_query = f"""
            MATCH path=(start:Station {{id: $start_spot}})-[:CONNECTED_TO*]->(end:Station {{id: $end_spot}})
            WITH path, length(path) as travel_time, size([r in relationships(path) | r]) - 1 as train_changes  
            WHERE 1=1 {travel_time_constraints}
            RETURN path, travel_time, train_changes
            ORDER BY {sort_f} {sort_order}
            LIMIT $limit
        """
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(neo_query, start_spot=starting_station_key.to_int(),
                                     end_spot=ending_station_key.to_int(), limit=limit, travel_time=travel_time)
                connections = []
                for record in result:
                    path = record["path"]
                    travel_time = record["travel_time"]
                    num_changes = record["train_changes"]
                    connections.append({
                        "path": path,
                        "travel_time": travel_time,
                        "num_changes": num_changes
                    })
                return connections

        except Exception as e:
            raise ValueError(f"An error occurred while searching for train connections: {e}")

    def get_train_current_status(self, train_key: TraitsKey) -> Optional[TrainStatus]:
        """
        Check the status of a train. If the train does not exist returns None
        """
        with self.rdbms_admin_connection.cursor() as cur:
            cur.execute(f"SELECT status FROM trains WHERE id = {train_key.to_int()}")
            result = cur.fetchone()
            if result:
                status = result[0]
                try:
                    return TrainStatus[status]
                except KeyError:
                    raise ValueError(f"Invalid train status: {status}")
            else:
                return None

    ########################################################################
    # Advanced Features
    ########################################################################

    def buy_ticket(self, user_email: str, connection, also_reserve_seats=True):
        """
                Given a train connection instance (e.g., on a given date/time), registered users can book tickets and optionally reserve seats. When the user decides to reserve seats, the system will try to reserve all the available seats automatically.
                We make the following assumptions:
                    - There is always a place on the trains, so tickets can be always bought
                    - The train seats are not numbered, so the system must handle only the number of passengers booked on a train and not each single seat.
                    - The system grants only those seats that are effectively available at the moment of request; thus, overbooking on reserved seats is not possible.
                    - Seats reservation cannot be done after booking a ticket.
                    - A user can only reserve one seat in each train at the given time.

                If the user does not exist, the method must raise a ValueError
                """
        if connection is None:
            raise ValueError("Connection cannot be None")
        cur = self.rdbms_admin_connection.cursor()
        try:
            # Check if the user exists
            user = self.utility.get_user_by_email(user_email)
            if not user:
                raise ValueError("User does not exist")
            # Find the user id and schedule id
            cur.execute(f"SELECT id FROM users WHERE email = '{user_email}'")
            user_id = cur.fetchone()[0]
            cur.execute(
                f"SELECT id FROM schedules WHERE train_id = {connection['train_id']} AND departure_date = '{connection['departure_date']}'")
            schedule_id = cur.fetchone()[0]

            # Check for available seats if also_reserve_seats is True
            if also_reserve_seats:
                cur.execute(f"SELECT capacity FROM trains WHERE id = {connection['train_id']}")
                train_capacity = cur.fetchone()[0]
                cur.execute(
                    f"SELECT SUM(number_of_seats) FROM seat_reservations WHERE ticket_id IN (SELECT id FROM tickets WHERE schedule_id = {schedule_id})")
                reserved_seats = cur.fetchone()[0] or 0
                available_seats = train_capacity - reserved_seats

                if available_seats <= 0:
                    raise ValueError("No available seats for reservation")

                # Buy the ticket
                cur.execute(
                    f"INSERT INTO tickets (user_id, schedule_id, purchase_date, price) VALUES ({user_id}, {schedule_id}, NOW(), {connection['price']})")
                ticket_id = cur.lastrowid

                # Reserve seats if also_reserve_seats is True
                if also_reserve_seats:
                    cur.execute(
                        f"INSERT INTO seat_reservations (ticket_id, number_of_seats) VALUES ({ticket_id}, 1)")

                self.rdbms_admin_connection.commit()

        except Exception as e:
            self.rdbms_admin_connection.rollback()
            raise ValueError(f"An error occurred during buying a ticket: {e}")

        finally:
            cur.close()

    def get_purchase_history(self, user_email: str) -> List:
        """
        Access Purchase History

        Registered users can list the history of their past purchases, including the starting and ending stations, the day/time, total price, and for each connection, the price and whether they reserved a seat.
        The purchase history is always represented in descending starting time (at the top the most recent trips).

        If the user is not registered, the list is empty
        """

        cur = self.rdbms_admin_connection.cursor()
        try:
            # Get the user id
            user = self.utility.get_user_by_email(user_email)
            if not user:
                return []
            user_id = user["id"]
            # Check if the user exists and return an empty list if not
            if not user_id:
                return []
            # Get the purchase history
            cur.execute(f"""
                    SELECT 
                        tickets.id as ticket_id,
                        schedules.start_train_station_id,
                        schedules.end_train_station_id,
                        schedules.departure_time,
                        schedules.departure_date,
                        schedules.arrival_time,
                        schedules.arrival_date,
                        tickets.purchase_date,
                        tickets.price,
                        seat_reservations.number_of_seats
                    FROM tickets
                    JOIN schedules ON tickets.schedule_id = schedules.id
                    LEFT JOIN seat_reservations ON tickets.id = seat_reservations.ticket_id
                    WHERE tickets.user_id = {user_id}
                    ORDER BY schedules.departure_date DESC, schedules.departure_time DESC
                """)

            purchase_history = []
            for row in cur.fetchall():
                purchase_history.append({
                    'ticket_id': row[0],
                    'start_station_id': row[1],
                    'end_station_id': row[2],
                    'departure_time': row[3],
                    'departure_date': row[4],
                    'arrival_time': row[5],
                    'arrival_date': row[6],
                    'purchase_date': row[7],
                    'total_price': row[8],
                    'reserved_seats': row[9] or 0
                })
            return purchase_history
        except Exception as e:
            raise ValueError(f"An error occurred during getting purchase history: {e}")
        finally:
            cur.close()

    ########################################################################
    # Admin Features:
    ########################################################################

    # Add and remove users
    def add_user(self, user_email: str, user_details) -> None:
        """
        Add a new user to the system with given email and details.
        Email format: <Recipient name>@<Domain name><top-level domain>
        See: https://knowledge.validity.com/s/articles/What-are-the-rules-for-email-address-syntax?language=en_US

        Raise a ValueError if the email has invalid format.
        Raise a ValueError if the user already exists
        """
        if not re.match(r"[^@]+@[^@]+\.[^@]+", user_email):
            raise ValueError("Invalid email format")

        cur = self.rdbms_admin_connection.cursor()
        try:
            user = self.utility.get_user_by_email(user_email)
            if user:
                raise ValueError("User already exists")

            if user_details is not None:
                cur.execute(
                    f"INSERT INTO users (email, password, is_admin) VALUES ('{user_email}', '{user_details['password']}', {user_details['is_admin']})")
            else:
                cur.execute(
                    f"INSERT INTO users (email, password, is_admin) VALUES ('{user_email}', NULL, NULL)")
            self.rdbms_admin_connection.commit()
        except Exception as e:
            print(f"An error occurred during adding a new user: {e}")
            raise

    def delete_user(self, user_email: str) -> None:
        """
        Delete the user from the db if the user exists.
        The method should also delete any data related to the user (past/future tickets and seat reservations)
        """
        cur = self.rdbms_admin_connection.cursor()
        try:
            # Check if the user exists
            user = self.utility.get_user_by_email(user_email)
            if not user:
                raise ValueError("User does not exist")
            # Delete the user
            cur.execute(f"DELETE FROM users WHERE email = '{user_email}'")
            # Delete all tickets of the user
            cur.execute(f"DELETE FROM tickets WHERE user_id = (SELECT id FROM users WHERE email = '{user_email}')")
            # Delete all seat reservations of the user
            cur.execute(
                f"DELETE FROM seat_reservations WHERE ticket_id IN (SELECT id FROM tickets WHERE user_id = (SELECT id FROM users WHERE email = '{user_email}'))")
            self.rdbms_admin_connection.commit()
        except Exception as e:
            print(f"An error occurred during deleting a user: {e}")

    def add_train(self, train_key: Optional[TraitsKey], train_capacity: int, train_status: TrainStatus) -> TraitsKey:
        """
        Add new trains to the system with given code.
        Raise a ValueError if the train already exists.
        Return the train key.
        IF THE TRAIN KEY IS NONE, GENERATE A NEW ONE
        """
        if train_capacity <= 0:
            raise ValueError("Train capacity must be greater than 0")
        cur = self.rdbms_admin_connection.cursor()
        if train_key is None:
            cur.execute("SELECT MAX(id) FROM trains")
            max_id = cur.fetchone()[0]
            new_id = 1 if max_id is None else max_id + 1
            train_key = TraitsKey(new_id)
        else:
            cur.execute(f"SELECT * FROM trains WHERE id = {train_key.to_int()}")
            if cur.fetchone():
                raise ValueError("Train already exists")

        status_str = train_status.name
        cur.execute(
            f"INSERT INTO trains (id, capacity, status) VALUES ({train_key.to_int()}, {train_capacity}, '{status_str}')")
        self.rdbms_admin_connection.commit()
        return train_key

    def update_train_details(self, train_key: TraitsKey, train_capacity: Optional[int] = None,
                             train_status: Optional[TrainStatus] = None) -> None:
        """
        Update the details of existing train if specified (i.e., not None), otherwise do nothing.
        """
        cur = self.rdbms_admin_connection.cursor()
        try:
            # Check if the train exists
            cur.execute(f"SELECT * FROM trains WHERE id = {train_key.to_int()}")
            if not cur.fetchone():
                raise ValueError("Train does not exist")

            # Update the train capacity
            if train_capacity is not None:
                cur.execute(f"UPDATE trains SET capacity = {train_capacity} WHERE id = {train_key.to_int()}")

            # Update the train status
            if train_status is not None:
                cur.execute(f"UPDATE trains SET status = '{train_status.name}' WHERE id = {train_key.to_int()}")

            self.rdbms_admin_connection.commit()
        except Exception as e:
            raise ValueError(f"An error occurred during updating train details: {e}")
        finally:
            cur.close()

    def delete_train(self, train_key: TraitsKey) -> None:
        """
        Deleting a train should ensure consistency! Reservations are cancelled, schedules/trips are cancelled, etc.
        Drop the train from the system. Note that all its schedules, reservations, etc. must be also dropped.
        """

        if train_key is None:
            raise ValueError("Invalid train key")

        cur = self.rdbms_admin_connection.cursor()
        try:
            # Delete the train
            cur.execute(f"DELETE FROM trains WHERE id = {train_key}")
            # Delete all schedules
            cur.execute(f"DELETE FROM schedules WHERE train_id = {train_key}")
            # Delete all tickets
            cur.execute(
                f"DELETE FROM tickets WHERE schedule_id IN (SELECT id FROM schedules WHERE train_id = {train_key})")
            # Delete all seat reservations
            cur.execute(
                f"DELETE FROM seat_reservations WHERE ticket_id IN (SELECT id FROM tickets WHERE schedule_id IN (SELECT id FROM schedules WHERE train_id = {train_key}))")
            self.rdbms_admin_connection.commit()
        except Exception as e:
            print(f"An error occurred during deleting a train: {e}")

    def add_train_station(self, train_station_key: TraitsKey, train_station_details) -> None:
        """
        Add a train station
        Duplicated are not allowed, raise ValueError
        """

        # Check if the train station already exists in RDBMS
        cur = self.rdbms_admin_connection.cursor()

        cur.execute(f"SELECT * FROM train_stations WHERE id = {train_station_key.to_int()}")
        if cur.fetchone():
            raise ValueError("Train station already exists in RDBMS")

        if train_station_details is None:
            train_station_details = {}

        # Set default values with unique identifiers
        train_station_details.setdefault('name', f'Unknown{train_station_key.to_int()}')
        train_station_details.setdefault('location', f'Unknown{train_station_key.to_int()}')

        if not train_station_key:
            raise ValueError("Invalid train station key")
        if not train_station_details['name'] or not train_station_details['location']:
            raise ValueError("Invalid train station details")

        # Check if a train station with the same name already exists
        cur.execute(f"SELECT * FROM train_stations WHERE name = '{train_station_details['name']}'")
        if cur.fetchone():
            raise ValueError("Train station with the same name already exists")

        try:
            # Add the station to RDBMS
            cur.execute(
                f"INSERT INTO train_stations (id, name, location) VALUES ({train_station_key.to_int()}, '{train_station_details['name']}', '{train_station_details['location']}')"
            )
            self.rdbms_admin_connection.commit()

            # Add the station to Neo4j
            with self.neo4j_driver.session() as session:
                session.run(
                    "CREATE (s:Station {id: $station_id, name: $name, location: $location})",
                    station_id=train_station_key.to_int(),
                    name=train_station_details['name'],
                    location=train_station_details['location']
                )
        except Exception as e:
            self.rdbms_admin_connection.rollback()
            print(f"An error occurred during adding a new train station: {e}")
            raise
        finally:
            cur.close()

    def connect_train_stations(self, starting_train_station_key: TraitsKey, ending_train_station_key: TraitsKey,
                               travel_time_in_minutes: int) -> None:
        """
        Connect to train station so trains can travel on them
        Raise ValueError if any of the stations does not exist
        Raise ValueError for invalid travel_times
        """
        # Check if the travel time is valid
        if travel_time_in_minutes <= 0:
            raise ValueError("Invalid travel time, minimum travel time is 1 minute")

        # Check if the stations exist
        neo_query = """
                        MATCH (s:Station {id: $station_id})
                        RETURN s
                    """
        for station_key in [starting_train_station_key, ending_train_station_key]:
            with self.neo4j_driver.session() as session:
                result = session.run(neo_query, station_id=station_key.to_int())
                if result.single() is None:
                    raise ValueError(f"Station with id {station_key.to_int()} does not exist")

        # Connect the stations in Neo4j
        neo_query = """
                        MATCH (start:Station {id: $start_point}), (end:Station {id: $end_point})
                        CREATE (start)-[:CONNECTED_TO {travel_time: $travel_time}]->(end)
                    """
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(neo_query, start_point=starting_train_station_key.to_int(),
                                     end_point=ending_train_station_key.to_int(),
                                     travel_time=travel_time_in_minutes)
                if result.consume().counters.relationships_created == 0:
                    raise ValueError("NEO4J: Failed to connect the train stations.")
        except Exception as e:
            print(f"An error occurred during connecting train stations: {e}")
            raise

        # Connect the stations in RDBMS
        cur = self.rdbms_admin_connection.cursor()
        cur.execute(
            f"INSERT INTO connections (start_station_id, end_station_id, travel_time_minutes) VALUES ({starting_train_station_key.to_int()}, {ending_train_station_key.to_int()}, {travel_time_in_minutes})"
        )
        self.rdbms_admin_connection.commit()
        cur.close()

    def add_schedule(self, train_key: TraitsKey,
                     starting_hours_24_h: int, starting_minutes: int,
                     stops: List[Tuple[TraitsKey, int]],  # [station_key, waiting_time]
                     valid_from_day: int, valid_from_month: int, valid_from_year: int,
                     valid_until_day: int, valid_until_month: int, valid_until_year: int) -> None:
        """
        Create a schedule for a given train.
        The schedule must have at least two stops, cannot connect the same station directly but can create "rings"
        Stops must correspond to existing stations
        Consecutive stops must be connected stations.
        Starting hours and minutes define when this schedule is active
        Validity dates must ensure that valid_from is in the past w.r.t. valid_until
        In case of error, raise ValueError
        Train key cannot be none
        """
        # IF THE TRAIN_KEY IS NONE
        # GIVE IT ONE WE GO INTO THIS
        cur = self.rdbms_admin_connection.cursor()
        if train_key is None:
            cur.execute("SELECT MAX(id) FROM trains")
            max_id = cur.fetchone()[0]
            new_id = 1 if max_id is None else max_id + 1
            train_key = TraitsKey(new_id)
            # Insert the train with its new key into the trains table
            cur.execute(
                f"INSERT INTO trains (id, status, capacity) VALUES ({train_key.to_int()}, 'OPERATIONAL', 100)"
            )
        else:
            cur.execute(f"SELECT * FROM trains WHERE id = {train_key.to_int()}")
            if cur.fetchone() is None:
                raise ValueError("Train does not exist")

        if len(stops) < 2:
            raise ValueError("The schedule must have at least two stops")

        for i in range(len(stops) - 1):
            start_station_key, _ = stops[i]
            end_station_key, _ = stops[i + 1]
            # Check if the stations are connected
            neo_query = """
                                MATCH (start:Station {id: $start_point})-[:CONNECTED_TO]->(end:Station {id: $end_point})
                                RETURN start, end
                            """
            with self.neo4j_driver.session() as session:
                result = session.run(neo_query, start_point=start_station_key.to_int(),
                                     end_point=end_station_key.to_int())
                if result.single() is None:
                    raise ValueError(
                        f"Stations {start_station_key.to_int()} and {end_station_key.to_int()} are not connected")

        # Check if the validity dates are correct
        if (valid_from_year, valid_from_month, valid_from_day) > (valid_until_year, valid_until_month, valid_until_day):
            raise ValueError("Valid from date must be in the past w.r.t. valid until date")

        # Add the schedule to RDBMS
        cur = self.rdbms_admin_connection.cursor()
        try:
            cur.execute(
                f"INSERT INTO schedules (train_id, start_train_station_id, end_train_station_id, departure_time, departure_date, arrival_time, arrival_date) VALUES ({train_key.to_int()}, {stops[0][0].to_int()}, {stops[-1][0].to_int()}, '{starting_hours_24_h}:{starting_minutes:02d}', '{valid_from_year}-{valid_from_month:02d}-{valid_from_day:02d}', '{starting_hours_24_h}:{starting_minutes:02d}', '{valid_until_year}-{valid_until_month:02d}-{valid_until_day:02d}')"
            )
            schedule_id = cur.lastrowid

            for i, (station_key, waiting_time) in enumerate(stops):
                cur.execute(
                    f"INSERT INTO schedule_stops (schedule_id, station_id, stop_order, waiting_time) VALUES ({schedule_id}, {station_key.to_int()}, {i + 1}, {waiting_time})"
                )

            self.rdbms_admin_connection.commit()
        except Exception as e:
            self.rdbms_admin_connection.rollback()
            print(f"An error occurred during adding a new schedule: {e}")
            raise
        finally:
            cur.close()
