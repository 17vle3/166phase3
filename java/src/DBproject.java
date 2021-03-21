/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Ship");
				System.out.println("2. Add Captain");
				System.out.println("3. Add Cruise");
				System.out.println("4. Book Cruise");
				System.out.println("5. List number of available seats for a given Cruise.");
				System.out.println("6. List total number of repairs per Ship in descending order");
				System.out.println("7. Find total number of passengers with a given status");
				System.out.println("8. < EXIT");
				
				switch (readChoice()){
					case 1: AddShip(esql); break;
					case 2: AddCaptain(esql); break;
					case 3: AddCruise(esql); break;
					case 4: BookCruise(esql); break;
					case 5: ListNumberOfAvailableSeats(esql); break;
					case 6: ListsTotalNumberOfRepairsPerShip(esql); break;
					case 7: FindPassengersCountWithStatus(esql); break;
					case 8: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddShip(DBproject esql) {//1
		
		//ID (list length -1)
		/*id INTEGER NOT NULL,
		make CHAR(32) NOT NULL,
		model CHAR(64) NOT NULL,
		age _YEAR_1970 NOT NULL,
		seats _SEATS NOT NULL,
		PRIMARY KEY (id)*/
		
		
		int id ;
		//get input for ID and put it into int id
		do {
		    try {
			System.out.print("\tEnter ID: $");
			id = Integer.parseInt(in.readLine());
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input");
			continue;
		    }
		} while (true);
		
		String make;
		//get input for make and put it into String make
		do {
		    try {
			System.out.print("\tEnter make: ");
			make = in.readLine();
			if(make.length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(make.length() > 32){
				throw new RuntimeException("input is too large");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage() );
			continue;
		    }
		} while (true);
		
		String model;
		//get input for model and put it into String model
		do {
		    try {
			System.out.print("\tEnter model: ");
			model = in.readLine();
			if(make.length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(make.length() > 64){
				throw new RuntimeException("input is too large");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		 
		int age;
		//get input for age and put it into int age
		do {
		    try {
			System.out.print("\tEnter age: ");
			age = Integer.parseInt(in.readLine());
			if(age < 0) {
				throw new RuntimeException("input has to be positive or zero");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		int seats;
		//get input for seats and put it into int seats
		do {
		    try {
			System.out.print("\tEnter seats: ");
			seats = Integer.parseInt(in.readLine());
			if(seats <= 0) {
				throw new RuntimeException("input has to be positive");
			}
			if(seats>= 500) {
				throw new RuntimeException("input cannot be greater or equal than 500");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);

		

		
		
		try{
			//input data we collected into Ship table
			String query = "INSERT INTO Ship (ID, make, model,age,seats)"; 	
			query += "VALUES (" + id + ", \'" + make + "\', \'" + model + "\', \'" +  age + "\', \'" + seats +"\');"; 
		 esql.executeUpdate(query);
		}catch(Exception e){
		 System.err.println (e.getMessage());
		}
		
	}

	public static void AddCaptain(DBproject esql) {//2
		/*id INTEGER NOT NULL,
		fullname CHAR(128),
		nationality CHAR(24),
		PRIMARY KEY (id)*/
		
		int id ;
		//get input for ID and put it into int id
		do {
		    try {
			System.out.print("\tEnter ID: $");
			id = Integer.parseInt(in.readLine());
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input");
			continue;
		    }
		} while (true);
		
		String fullname;
		//get input for fullname and put it into String fullname
		do {
		    try {
			System.out.print("\tEnter fullname: ");
			fullname = in.readLine();
			if(fullname.length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(fullname.length() > 128){
				throw new RuntimeException("input is too large");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		String nationality;
		//get input for nationality and put it into String nationality
		do {
		    try {
			System.out.print("\tEnter nationality: ");
			nationality = in.readLine();
			if(nationality.length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(nationality.length() > 24){
				throw new RuntimeException("input is too large");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		
		try{
			//input a new captain into the Captain table
			String query = "INSERT INTO Captain (ID, fullname, nationality) VALUES (" + id + ", \'" + fullname + "\', \'" + nationality +"\');"; 
		 esql.executeUpdate(query);

	      }catch(Exception e){
		 System.err.println (e.getMessage());
	      }
		
		
		
	}

	public static void AddCruise(DBproject esql) {//3
		/*cnum INTEGER NOT NULL,
		cost _PINTEGER NOT NULL,
		num_sold _PZEROINTEGER NOT NULL,
		num_stops _PZEROINTEGER NOT NULL,
		actual_departure_date DATE NOT NULL,
		actual_arrival_date DATE NOT NULL,
		arrival_port CHAR(5) NOT NULL,-- PORT CODE --
		departure_port CHAR(5) NOT NULL,-- PORT CODE --
		PRIMARY KEY (cnum)*/
		
		//date format so it matches date of the data
		DateTimeFormatter Date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
		
		int cnum ;
		//get input for cnum and put it into  int cnum
		do {
		    try {
			System.out.print("\tEnter cnum: $");
			cnum = Integer.parseInt(in.readLine());
			break;
		    }
		    catch (Exception excpt) {
			System.out.println("\tInvalid input");
			continue;
		    }
		} while (true);
		
		int cost;
		//get input for cost and put it into  int cost
		do {
		    try {
			System.out.print("\tEnter cost: $");
			cost = Integer.parseInt(in.readLine());
			if(cost < 0) {
				throw new RuntimeException("input has to be positive");
			}
			if(cost == 0) {
				throw new RuntimeException("input cannot be zero");
			}
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		int num_sold;
		//get input for num_sold and put it into  int num_sold
		do {
		    try {
			System.out.print("\tEnter num_sold: $");
			num_sold = Integer.parseInt(in.readLine());
			if(num_sold < 0) {
				throw new RuntimeException("input has to be positive or zero");
			}
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		int num_stops;
		//get input for num_stops and put it into  int num_stops
		do {
		    try {
			System.out.print("\tEnter num_stops: $");
			num_stops = Integer.parseInt(in.readLine());
			if(num_stops < 0) {
				throw new RuntimeException("input has to be positive or zero");
			}
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		String actual_departure_date ;
		//get input for actual_departure_date and put it into  String actual_departure_date then convert it to the date type
		do {
		    try {
			System.out.print("\tEnter actual_departure_date [yyyy-MM-dd HH:mm]: $");
			actual_departure_date = in.readLine();
			LocalDate localadd = LocalDate.parse(actual_departure_date, Date);
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		String actual_arrival_date  ;
		//get input for actual_arrival_date and put it into  String actual_arrival_date then convert it to the date type
		do {
		    try {
			System.out.print("\tEnter actual_arrival_date  [yyyy-MM-dd HH:mm]: $");
			actual_arrival_date  = in.readLine();
			LocalDate localadd = LocalDate.parse(actual_arrival_date , Date);
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		String arrival_port   ;
		//get input for arrival_port and put it into String arrival_port 
		do {
		    try {
			System.out.print("\tEnter arrival_port : $");
			arrival_port   = in.readLine();
			if(arrival_port.length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(arrival_port.length() > 5){
				throw new RuntimeException("input is too large");
			}
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		String departure_port    ;
		//get input for departure_port and put it into String departure_port 
		do {
		    try {
			System.out.print("\tEnter departure_port  : $");
			departure_port    = in.readLine();
			if(departure_port .length() < 1) {
				throw new RuntimeException("empty input");
			}
			else if(departure_port .length() > 5){
				throw new RuntimeException("input is too large");
			}
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);
		
		
		try{
			//Use the values and add a Cruise to the Cruise table 
		String query = "INSERT INTO Cruise (cnum, cost, num_sold, num_stops,actual_departure_date,actual_arrival_date,arrival_port, departure_port)"; 
		query += " VALUES (" + cnum  + ", \'" + cost  + "\', \'" + num_sold + "\', \'" + num_stops + "\', \'" +actual_departure_date;
		query += "\', \'" +   actual_arrival_date  + "\', \'" + arrival_port   + "\', \'" + departure_port+"\');"; 		 esql.executeUpdate(query);
			

	      }catch(Exception e){
		 System.err.println (e.getMessage());
	      }
		

	}


	public static void BookCruise(DBproject esql) {//4
		int ccid, cid;
		int rnum=0;
		String  userInput, query, status;
		

		do {
			System.out.print("\tEnter Customer ID : ");
			try {
				ccid = Integer.parseInt(in.readLine());
				break;
			}
			catch(Exception e) {
				System.out.println("Invalid input. Exception: " + e.getMessage());
				continue;
			}
		}while(true);

		do{
			System.out.print("\tEnter Cruise ID: ");
			try {
				cid = Integer.parseInt(in.readLine());
				break;
			}
			catch(Exception e) {
				System.out.println("Invalid input. Exception: " + e.getMessage());
			}
		}while(true) ;

		try {
			query = "SELECT status FROM Reservation WHERE ccid = " + ccid + "AND cid = " + cid + ";";
			if(esql.executeQueryAndPrintResult(query) == 0) {
				while(true) {
					System.out.println("\tNo reservation yet. Do you want to book a cruise? 'yes' or 'no': ");
					try {
						userInput = in.readLine();
						if(userInput.equals("yes") || userInput.equals("y")) {
							
							try {
								query = "SELECT R.rnum FROM Reservation R;";
								List <List<String>> Reservation_num  = esql.executeQueryAndReturnResult(query);
								rnum = Reservation_num.size() + 1;
							}
							catch(Exception e) {
								System.err.println(e.getMessage());
							}
							
							
							
							
							try {
								query = "SELECT T1.cnum, T1.seats-T2.num_sold as availseats FROM (SELECT C1.cnum, S1.seats FROM Cruise C1, CruiseInfo CI1, Ship S1 WHERE CI1.ciid = C1.cnum AND CI1.ship_id = S1.id) AS T1, (SELECT C2.cnum, C2.num_sold FROM Cruise C2 GROUP BY C2.cnum) AS T2 WHERE T1.cnum = T2.cnum AND T1.cnum = " + cid + ";";
								if(esql.executeQueryAndPrintResult(query)  == 0) {
									status = "W";
									System.out.print("There are no seats open on this cruise. You have been waitlisted.");
								}
								else {
									status = "C";
									System.out.print("Your reservation has been confirmed");
								}
								
									
							}
							catch(Exception e) {
								System.err.println(e.getMessage());
								continue;
							}
								
							
							
								
							try {
								query = "INSERT INTO Reservation (rnum, ccid, cid, status) VALUES (" + rnum + ", " + ccid + ", " + cid + ", '" + status + "');";
                                    				esql.executeUpdate(query);
								
								//query = "SELECT R.rnum, R.ccid, R.cid, R.status FROM Reservation R;";
								//esql.executeQueryAndPrintResult(query);

							}
							catch(Exception e) {
								System.err.println("Invalid input" + e.getMessage());
								continue;
							}
							
						}
						else if(userInput.equals("no") || userInput.equals("n")) {
							break;
						}
						else {
							throw new RuntimeException("Invalid input. Input must be 'yes', 'y', 'no', or 'n'");
						}
					
					}
					catch(Exception e) {
						System.out.println("Invalid input. Exception: " + e.getMessage());
						continue;
					}
					break;
				}
			}
			else { 
				while(true) {
					 try {
                            			System.out.println("Reservation found.");
									
						query = "SELECT status FROM Reservation WHERE ccid = " + ccid + "AND cid = " + cid + ";";
						status = esql.executeQueryAndReturnResult(query).get(0).get(0);
						//if(status == "R") {
						//	System.out.print("You have reserved this cruise, would you like to confirm your reservation? Input yes or no.");
						//}
						//else {
							System.out.print("Status: " + status + "\n");
						//	break;
						//			

						System.out.print("Would you like to change the status of your reservation. Input yes or no.");
                            			userInput = in.readLine();
                            			if(userInput.equals("yes") || userInput.equals("y")) {
                                			while (true) {
                                    				System.out.print("Input new Reservation Status(W, C, or R): " );
                                    				try {
                                        				status = in.readLine();
                                        				if(!status.equals("W")  && !status.equals("C") && !status.equals("R")) {
                                            					throw new RuntimeException("Your input is invalid! Status can only be W, C, or R");
                                        				}
                                        				break;
                                    				}
                                    				catch(Exception e) {
                                        				System.out.println("Input is invalid. Exception: " + e.getMessage());
                                       					 continue;
                                    				}
							}
                                			

                                			try {
                                    				query = "UPDATE Reservation SET status = \'" + status + "\' WHERE ccid = " + ccid + " AND cid = " + cid + ";";
                                    				esql.executeQuery(query);

								//query = "SELECT R.rnum, R.ccid, R.cid, R.status FROM Reservation R WHERE R.ccid = " + ccid + ";";
								//esql.executeQueryAndReturnResult(query);
                                			}
                               	 			catch (Exception e) {
                                    				System.err.println(e.getMessage());
                                			}
						}
						
                            			else if (userInput.equals("no") || userInput.equals("n")) {
							continue;
						}
						else {
                                			throw new RuntimeException("Invalid input. Input must be 'yes', 'y', 'no', or 'n'.");
                            			}
                            			break;
                        		}
                        		catch(Exception e){
                            			System.out.println("Your input is invalid! Your exception is: " + e.getMessage());
                            			continue;
                        		}
                    		}
                	}
            
            }
            catch(Exception e) {
                System.err.println(e.getMessage());
            }
		
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5
		// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
		//get seats from ship
		//get date from cruise
		//find num sold

		DateTimeFormatter Date = DateTimeFormatter.ofPattern("yyyy-dd-MM HH:mm");

		int cnum ;
		do {
		    try {
			System.out.print("\tEnter cnum: $");
			cnum = Integer.parseInt(in.readLine());
			break;
		    }
		    catch (Exception excpt) {
			System.out.println("\tInvalid input");
			continue;
		    }
		} while (true);
		
		String actual_departure_date ;
		do {
		    try {
			System.out.print("\tEnter departure_date [yyyy-MM-dd HH:mm]: $");
			actual_departure_date = in.readLine();
			LocalDate localadd = LocalDate.parse(actual_departure_date, Date);
			    break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input: " + e.getMessage());
			continue;
		    }
		} while (true);

		try {
			//T1 is  number of seats
			//T2 is number of sold
			//subtract them to get availible seats.
			//there is one date per cruise therefore we do not use the input for date.
			//String query = "SELECT T1.seats-T2.num_sold FROM Ship S, Cruise C, (SELECT C1.cnum, S1.seats FROM Cruise C1, CruiseInfo CI1, Ship S1 WHERE CI1.ciid = C1.cnum AND CI1.ship_id = S1.id) AS T1, (SELECT C2.cnum, C2.num_sold FROM Cruise C2 GROUP BY C2.cnum) AS T2 WHERE C.cnum = T1.cnum AND T1.cnum = T2.cnum AND T1.cnum = " + cnum + " AND C.actual_departure_date  = " +actual_departure_date + ";";
			String query = "SELECT DISTINCT T1.seats-T2.num_sold AS availSeats FROM Ship S, (SELECT C1.cnum, S1.seats FROM Cruise C1, CruiseInfo CI1, Ship S1 WHERE CI1.ciid = C1.cnum AND CI1.ship_id = S1.id) AS T1, (SELECT C2.cnum, C2.num_sold FROM Cruise C2 GROUP BY C2.cnum) AS T2 WHERE  T1.cnum = T2.cnum AND T1.cnum = " + cnum + " ;";

			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e) {
			System.err.print(e.getMessage());
		}
		
		
	}

	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
		//count * 
		//from repairs
		//group by ships
		//order by repairs DESC
		
		try {
			//get ship ID and count the number of existing ship_ids from all of the repairs. Order the count by descending.
			String query =" SELECT ship_id, COUNT(ship_id) FROM Repairs R GROUP BY ship_id ORDER BY COUNT(ship_id) DESC;";			
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e) {
			System.err.print(e.getMessage());
		}
		
	}

	
	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		//get input status (has to be W,C,R)
		//count*
		//passengers 
		//from reservations
		//where (statuse = input)
		String input;
		do {
			//get status
		    try {
			System.out.print("\tEnter status: ");
			input = in.readLine();
			if(!input.equals("W") && !input.equals("C") && !input.equals("R")){ //if the input isn't valid, throw exception
				throw new RuntimeException("");
			}
			break;
		    }
		    catch (Exception e) {
			System.out.println("\tInvalid input. Please input a \"W\" , \"C\" or \"R\". " + e.getMessage());
			continue;
		    }
		} while (true);
		
		try {
			//count number of reservations with same input status
			String query ="SELECT COUNT(*) FROM Reservation WHERE status = '" + input + "';";			
			esql.executeQueryAndPrintResult(query);
		}
		catch(Exception e) {
			System.err.print(e.getMessage());
		}
	}
}
