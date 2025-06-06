import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types;

import java.util.Properties;
import java.util.Scanner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


//Main
public class SQLServer{
	static Connection connection;

	public static void main(String[] args) throws Exception{
		//setup
		MyDatabase db = new MyDatabase();
		runConsole(db);
	}
	
	public static void runConsole(MyDatabase db)
	{
		Scanner console = new Scanner(System.in);
		printWelcome();
		String line = "";
		String[] parts;
		String arg = "";
		int mode = 0;
		int protectMode = 0;
		while (line != null && !line.equals("q") && !line.equals("quit"))
		{
			parts = splitLine(line);
			arg = findArg(line);
			if(mode == 0)
			{
				if(parts[0].equals("1"))
				{
					mode = 1;
				}
				else if(parts[0].equals("2"))
				{
					mode = 2;
				}
				else if(parts[0].equals("3"))
				{
					mode = 3;
				}
				else
				{
					System.out.println("Enter 1 for Browse Mode, 2 for Search Mode, or 3 for Maintenance Mode.");
				}
			}
			else if(parts.length > 0)
			{
				if (parts[0].equals("help") || argCheck(arg)) {
					printHelp(mode);	
				}
				else if(mode == 1)//Browse Mode. All "lookup" and "print" queries go here
				{
					if(parts[0].equals("2"))
					{
						mode = 2;
					}
					else if(parts[0].equals("3"))
					{
						mode = 3;
					}
					else if(parts[0].equals("w") && parts.length == 2)
					{
						db.searchWard(parts[1]);
					}
					else if(parts[0].equals("a") && parts.length == 2)
					{
						if(parts[1].equals("w"))
						{
							db.allWards();
						}
						else if(parts[1].equals("c"))
						{
							db.allCouncillors();
						}
						else if(parts[1].equals("n"))
						{
							db.allNeighbourhoods();
						}
						else if(parts[1].equals("t"))
						{
							db.allThirdParty();
						}
						else if(parts[1].equals("b"))
						{
							db.allBusinessOwners();
						}
						else { System.out.println("Type help for help"); }
					}
					else if(parts[0].equals("c") && parts.length == 3)
					{
						if(parts[1].equals("n"))
						{
							db.searchCouncillor(parts[2]);
						}
						else if(parts[1].equals("h"))
						{
							db.councillorByNBH(parts[2]);
						}
						else if(parts[1].equals("y"))
						{
							db.searchCouncilYear(parts[2]);
						}
						else { System.out.println("Usage: c n <name> OR c h <neighbourhood> OR c y <YYYY>"); }
					}
					else if(parts[0].equals("b") && parts.length == 3)
					{
						if(parts[1].equals("n"))
						{
							db.searchBusinessName(parts[2]);
						}
						else if(parts[1].equals("own"))
						{
							db.businessByOwner(parts[2]);
						}
						else { System.out.println("Usage: b n <name> OR b own <ownerName>"); }
					}

					else 
					{
						System.out.println("Type help for help");
					}
				}
				else if(mode == 2)//Search Mode. All "Show" and "Aggregate" queries go here
				{
					if(parts[0].equals("1"))
					{
						mode = 1;
					}
					else if(parts[0].equals("3"))
					{
						mode = 3;
					}
					else if(parts[0].equals("c") && parts.length == 3)
					{
						if(parts[1].equals("e"))
						{
							db.expensesByCouncillor(parts[2]);
						}
						else if(parts[1].equals("g"))
						{
							db.giftsByCouncillor(parts[2]);
						}
						else if(parts[1].equals("l"))
						{
							db.lobbiesByCouncillor(parts[2]);
						}
						else { System.out.println("Usage: c e <CouncillorID> OR c g <CouncillorID> OR c l <CouncillorID>"); }
					}
					else if(parts[0].equals("r"))
					{
						if(parts[1].equals("e") && parts.length == 4)
						{
							db.expensesByDate(null, parts[2], parts[3]);
						}
						else if(parts[1].equals("e") && parts.length == 5)
						{
							db.expensesByDate(parts[4], parts[2], parts[3]);
						}
						else if(parts[1].equals("g") && parts.length == 4)
						{
							db.giftsByDate(null, parts[2], parts[3]);
						}
						else if(parts[1].equals("g") && parts.length == 5)
						{
							db.giftsByDate(parts[4], parts[2], parts[3]);
						}
						else { System.out.println("Usage: r e|g|l <YYYYMMDD> <YYYYMMDD> (CID)"); }
					}
					else if(parts[0].equals("n") && parts.length == 3)
					{
						db.perfectCouncillorsByDate(parts[1], parts[2]);
					}
					else if(parts[0].equals("v") && parts.length == 3)
					{
						if(parts[1].equals("e"))
						{
							db.totalPriceExpenses(parts[2]);
						}
						else if(parts[1].equals("g"))
						{
							db.totalPriceGifts(parts[2]);
						}
						else { System.out.println("Usage: v e <CouncillorID> OR v g <CouncillorID>"); }
					}
					else if(parts[0].equals("t") && parts.length == 2)
					{
						if(parts[1].equals("e"))
						{
							db.bigSpenders();
						}
						else { System.out.println("Usage: t e"); }
					}
					else if(parts[0].equals("s") && parts.length == 1)
					{
						db.electionGifts();
					}
					else if(parts[0].equals("g") && parts.length == 1)
					{
						db.giftGap();
					}
					else if(parts[0].equals("l") && parts.length == 1)
					{
						db.bigGivers();
					}

					else 
					{
						System.out.println("Type help for help");
					}
				}
				else if(mode == 3)//Maintenance Mode. Delete and restore queries go here
				{
					if(parts[0].equals("1") && protectMode == 0)
					{
						mode = 1;
					}
					else if(parts[0].equals("2") && protectMode == 0)
					{
						mode = 2;
					}
					else if(parts[0].equals("deleteTables") && protectMode == 0) 
					{
						System.out.println("\nYou are about to delete all data. Type 'confirm' to procced");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("Deleting data...");
							db.deleteTables();
							protectMode = 1;
						}
						else {
							System.out.println("Delete canceled.");
						}
					}
					else if(parts[0].equals("populateTables") && protectMode == 1) 
					{
						System.out.println("\nYou are about to populate the database. Type 'confirm' to procced");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("Populating database...");
							db.createTables();
							db.fillTables();
							protectMode = 0;
						}
						else {
							System.out.println("Populate canceled.");
						}	
					}
					else if(protectMode == 1)
					{
						System.out.println("Please populate the database before continuing to use the program.");
					}

					else 
					{
						System.out.println("Type help for help");
					}
				}
				
				else 
				{
					System.out.println("Illegal Mode Violation - Something has gone wrong.");
					console.close();
				}
			}
			
			System.out.print("db > ");
			line = console.nextLine();
		}
		console.close();
	}

	//Info Functions
	private static void printWelcome()
	{
		System.out.println("");
		System.out.println("Welcome to the Winnipeg Council Transparency Database!");
		System.out.println("Here you can browse data on Council Members affairs such as gifts received.");
		System.out.println("Please note that Winnipeg Council Members are not required to disclose lobby activities.");
		System.out.println("-----------");
		System.out.println("How to use:");
		System.out.println("-----------");
		System.out.println("Enter '1' for Browse Mode if you want to lookup data");
		System.out.println("Enter '2' for Search Mode if you want to search for relationships in data");
		System.out.println("Enter '3' for Maintenance Mode for clearing and re-populating the database");
		System.out.println("Enter 'help' at any time for detailed help, or 'q' to quit");
		System.out.println("-----------");
	}
	private static void printHelp(int mode) 
	{
		if(mode == 1)//Browse Mode
		{
			System.out.println("Browse Mode Commands:\n");
			System.out.println("Print all Wards: \na w\n");
			System.out.println("Print all Councillors\na c\n");
			System.out.println("Print all Neighbourhoods\na n\n");
			System.out.println("Print all registered Third-Parties (WARNING: May be large!)\na t\n");
			System.out.println("Print all registered business owners\na b\n");
			System.out.println("-\n");
			System.out.println("Lookup Ward data by <name>:\nw <name>\n");
			System.out.println("-\n");
			System.out.println("Lookup Councillor data by <name>:\nc n <name>\n");
			System.out.println("Lookup Councillor data by <neighbourhoodName>\nc h <neighbourhoodName>\n");
			System.out.println("Lookup Councillor data by <YYYY> (year)\nc y <YYYY>\n");
			System.out.println("-\n");
			System.out.println("Lookup Business data by <name>\nb n <name>\n");
			System.out.println("Lookup Business data by <ownerName>\nb own <ownerName>\n");
			System.out.println("-----");
			System.out.println("Enter 2 to enter Search Mode, 3 for Maintenance Mode, or q to quit");
		}
		else if(mode == 2)//Search Mode
		{
			System.out.println("Search Mode Commands:\n");
			System.out.println("Get Councillor expenses by <CID>:\nc e <CID>\n");
			System.out.println("Get Councillor gifts received by <CID>:\nc g <CID\n");
			System.out.println("Get Councillor lobby activities by <CID>:\nc l <CID>\n");
			System.out.println("-\n");
			System.out.println("Show all expenses in a date range:\nr e <YYYYMMDD> <YYYYMMDD\n");
			System.out.println("Show all gifts in a date range:\nr g <YYYYMMDD> <YYYYMMDD>\n"); 
			System.out.println("-\n");
			System.out.println("Show Councillors that have not participated in any lobby activities or received any gifts in a date range:\nn <YYYYMMDD> <YYYYMMDD>\n");
			System.out.println("-\n");
			System.out.println("Get expendature total by <CID>:\nv e <CID>\n");
			System.out.println("Get total value of gifts received by <CID>:\nv g <CID>\n");
			System.out.println("-\n");
			System.out.println("Show Top 10 councillors that spend the most:\nt e \n");
			System.out.println("-\n");
			System.out.println("Show Top 10 gifts that were given closest to an election:\ns\n");
			System.out.println("Show Top 10 gifts with the largest gap between date recorded and date received:\ng\n");
			System.out.println("Show Top 10 third-parties that have given gifts and/or preformed lobby activities:\nl\n");
			System.out.println("-----");
			System.out.println("Enter 1 to enter Browse Mode, 3 for Maintenance Mode, or q to quit");
		}
		else if(mode == 3)//Maintenance Mode
		{
			System.out.println("Maintenance Mode Commands:");
			System.out.println("deleteTables: Clear all data");
			System.out.println("populateTables: Restore all data");
			System.out.println("-----");
			System.out.println("Enter 1 to enter Browse Mode, 2 for Search Mode, or q to quit");
		}
	}

	//Utillity Functions
	private static String[] splitLine(String line) 
	{
		String[] parts;
		parts = line.split("\\s+");
		return parts;
	}
	private static String findArg(String line)
	{
		String arg = "";
		if (line.indexOf(" ") > 0) {
			arg = line.substring(line.indexOf(" ")).trim();
		}
		return arg;
	}
	private static boolean argCheck(String arg)
	{ //more rudimentary sql injection defence, others from preparedStatements
		return (arg.contains("-") || arg.contains("\'"));
	}
	
}

//Database connection
class MyDatabase {
	private Connection connection; 
	DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("MMMM dd yyyy");
	DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private final String SEPARATOR_LINE = "-----------------------------------------------------------------------------------";

	public MyDatabase(){//constructor to establish connection to database
		//Reads config file auth.cfg for username and password to connect to database on Uranium
		Properties prop = new Properties();
        String fileName = "auth.cfg";
        try {
            FileInputStream configFile = new FileInputStream(fileName);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file.");
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null){
            System.out.println("Username or password not provided.");
            System.exit(1);
        }

        String connectionURL =
                "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password="+ password +";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        ResultSet resultSet = null;
		try {
			System.out.println("Attempting connection...");
			connection = DriverManager.getConnection(connectionURL);
			deleteTables();
			createTables();
			fillTables();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	//CREATE TABLES 
	public void createTables() {
		Statement stmt = null;
		try {
			connection.setAutoCommit(false);
			
			stmt = connection.createStatement();
			
			// TABLES ------
				// In correct order

			// Create Ward table
			stmt.executeUpdate(
				"CREATE TABLE Ward (" +
				" WID INT PRIMARY KEY IDENTITY(1,1), " +
				" WardE VARCHAR(255), " +
				" WardF VARCHAR(255)" +
				");"
			);
			
			// Create Councillors table
			stmt.executeUpdate(
				"CREATE TABLE Councillors (" +
				" CID INT PRIMARY KEY IDENTITY(1,1), " +
				" WID INT, " +
				" Present BIT, " +
				" Name VARCHAR(50), " +
				" Phone VARCHAR(50), " +
				" Fax VARCHAR(50), " +
				" WebsiteURL VARCHAR(255), " +
				" FOREIGN KEY (WID) REFERENCES Ward(WID)" +
				");"
			);
			
			// Create Election table
			stmt.executeUpdate(
				"CREATE TABLE Election (" +
				" ElectionDate DATE, " +
				" CID INT, " +
				" Type VARCHAR(100), " +
				" Position VARCHAR(100), " +
				" PRIMARY KEY (ElectionDate, CID), " +
				" FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// Create YearsServed
			stmt.executeUpdate(
				"CREATE TABLE YearsServed (" +
				" CID INT, " +
				" Year INT, " +
				" PRIMARY KEY (CID, Year), " +
				" FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// Create CouncilNeighbourhoods table
			stmt.executeUpdate(
				" CREATE TABLE CouncilNeighbourhoods (" +
				" CID INT, " +
				" Area VARCHAR(255), " +
				" PRIMARY KEY (CID, Area), " +
				" FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// Create ThirdParty table
			stmt.executeUpdate(
				" CREATE TABLE ThirdParty (" +
				" TID INT PRIMARY KEY IDENTITY(1,1), " +
				" Name VARCHAR(255), " +
				" Address VARCHAR(255), " +
				" Phone VARCHAR(50), " +
				" Email VARCHAR(255), " +
				" isBusiness BIT, " +
				" isVendor BIT" +
				");"
			);
			
			// Create Gift table (for gift details)
			stmt.executeUpdate(
				" CREATE TABLE Gift (" +
				" GID INT PRIMARY KEY, " +
				" Description VARCHAR(255), " +
				" Value DECIMAL(10,2)" +
				");"
			);
			
			// Create BusinessOwner table
			stmt.executeUpdate(
				" CREATE TABLE BusinessOwner (" +
				" OwnerID INT PRIMARY KEY IDENTITY(1,1), " +
				" Name VARCHAR(255), " +
				" TID INT, " +
				" FOREIGN KEY (TID) REFERENCES ThirdParty(TID)" +
				");"
			);
			
			// Create Represents table
			stmt.executeUpdate(
				" CREATE TABLE Represents (" +
				" CID INT, " +
				" WID INT, " +
				" PRIMARY KEY (CID, WID), " +
				" FOREIGN KEY (CID) REFERENCES Councillors(CID), " +
				" FOREIGN KEY (WID) REFERENCES Ward(WID)" +
				");"
			);
			
			// Create Participates table
			stmt.executeUpdate(
				" CREATE TABLE Participates (" +
				" Participant VARCHAR(255), " +
				" ElectionDate DATE, " +
				" Votes INT, " +
				" PRIMARY KEY (Participant, ElectionDate), " +
				");"
			);
			
			// Create Gifts table (for gift transactions)
			stmt.executeUpdate(
				" CREATE TABLE Gifts (" +
				" GID INT, " +
				" DateRecorded DATE, " +
				" Councillor INT, " +
				" RecipientSelf BIT, " +
				" RecipientDependent BIT, " +
				" RecipientStaff BIT, " +
				" Source INT, " +
				" DateGifted DATE, " +
				" Reason VARCHAR(3000), " +
				" Intent VARCHAR(1000), " +
				" PRIMARY KEY (GID, Councillor), " +
				" FOREIGN KEY (GID) REFERENCES Gift(GID), " +
				" FOREIGN KEY (Councillor) REFERENCES Councillors(CID), " +
				" FOREIGN KEY (Source) REFERENCES ThirdParty(TID)" +
				");"
			);
			
			// Create BuysFrom table
			stmt.executeUpdate(
				"CREATE TABLE BuysFrom (" +
				" PurchaseID INT PRIMARY KEY IDENTITY(1,1), " +
				" CID INT, " +
				" Date DATE, " +
				" Vendor INT, " +
				" ExpenseType VARCHAR(100), " +
				" Description VARCHAR(255), " +
				" Account VARCHAR(100), " +
				" Amount DECIMAL(10,2), " +
				" Department VARCHAR(100), " +
				" FOREIGN KEY (CID) REFERENCES Councillors(CID), " +
				" FOREIGN KEY (Vendor) REFERENCES ThirdParty(TID)" +
				");"
			);

			// Create Owns table
			stmt.executeUpdate(
				" CREATE TABLE Owns ("+
				" OwnerID INT,"+
				" Business INT,"+
				" PRIMARY KEY (OwnerID, Business),"+
				" FOREIGN KEY (OwnerID) REFERENCES BusinessOwner(OwnerID),"+
				" FOREIGN KEY (Business) REFERENCES ThirdParty(TID)"+
				");"
			);

			//Create lobbies table
			stmt.executeUpdate(
				"CREATE TABLE Lobbies ("+
				" LID INT PRIMARY KEY IDENTITY(1,1),"+
				" Owner INT,"+
				" Business INT,"+
				" ClientBusiness VARCHAR(1000),"+
				" CID INT,"+
				" LobbyDate DATE,"+
				" Subject VARCHAR(1000),"+
				" IntendedOutcome VARCHAR(2000),"+
				" FOREIGN KEY (Owner) REFERENCES BusinessOwner(OwnerID),"+
				" FOREIGN KEY (Business) REFERENCES ThirdParty(TID),"+
				" FOREIGN KEY (CID) REFERENCES Councillors(CID)"+
				");"
				);
			
			// Commit the transaction if everything is successful.
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				System.out.println("Rolled back due to error.");
			} catch (SQLException rollbackEx) {
				rollbackEx.printStackTrace();
			}
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				connection.setAutoCommit(true);
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}

	public void fillTables(){
		String councilCsv = "Council_Data.csv";
		String electionCsv = "Elections.csv";
		String giftsCsv = "Gifts.csv";
		String expensesCsv = "Council_Member_Expenses.csv";
		String lobbyistCsv = "Lobbyist_Registry.csv";

		try{
			connection.setAutoCommit(false);
			//hashmaps of data to their primary keys
			Map<String, Integer> thirdPartyMap = new HashMap<String, Integer>();
			//should change the other ones to pass by reference 
			// 1. Wards
			Map<String, Integer> wardMap = populateWards(councilCsv);

			// 2. Councillors
			Map<String, Integer> councillorMap = populateCouncillors(councilCsv, wardMap);

			// 3. CouncilNeighbourhoods
			populateCouncilNeighbourhoods(councilCsv, councillorMap);

			// 4. Represents
			populateRepresentsTable(councilCsv, councillorMap, wardMap);

			// 5. Elections
			populateElections(electionCsv, councillorMap);

			// 6. YearsServed
			populateYearsServed(councilCsv, councillorMap);

			// 7. Participates
			populateParticipates(electionCsv);

			System.out.println("Almost there...");

			// 8. ThirdParty - Vendors from Expenses
			populateThirdPartyVendors(expensesCsv, thirdPartyMap);

			// 9. ThirdParty - Businesses from Lobbyist Registry
			populateThirdPartyBusinesses(lobbyistCsv, thirdPartyMap);

			//third party who gives gifts
			populateThirdPartyGift(giftsCsv, thirdPartyMap);

			// 10. Business Owners
			Map<String, Integer> ownerMap = populateBusinessOwners(lobbyistCsv, thirdPartyMap);
			// 11. BuysFrom
			populateBuysFrom(expensesCsv, councillorMap, thirdPartyMap);
			// 12. Owns
			populateOwns(lobbyistCsv, ownerMap, thirdPartyMap);

			// 13. Gifts (Gift + Gifts tables)
			populateGiftTable(giftsCsv);
			populateGifts(giftsCsv, councillorMap, thirdPartyMap);
			// 14. Lobbies
			populateLobbies(lobbyistCsv, ownerMap, thirdPartyMap, councillorMap);

			connection.commit();

			System.out.println("All tables populated successfully.");
		}
		catch (IOException | SQLException e) {
			try {
				connection.rollback(); 
				System.err.println("Rolled back changes.");
				e.printStackTrace();
			} catch (SQLException ex) {
				System.err.println("Failed to rollback: you have terrible luck " + ex.getMessage());
			}
		} finally {
			try{
				connection.setAutoCommit(true);
			}// Restore default
			catch (SQLException c){
				System.err.println("couldnt reset autocommit");
			}
		}
	}


	//DELETE THE TABLES
	//do it in an order that doesnt break foreign key constrainsts
	public void deleteTables() {
		String[] dropOrder = {
			"Lobbies",
			"Owns",
			"Participates",
			"Gifts",
			"BuysFrom",
			"Represents",
			"BusinessOwner",
			"CouncilNeighbourhoods",
			"YearsServed",
			"Election",
			"Councillors",
			"Gift",
			"ThirdParty",
			"Ward"
		};
	
		try (Statement stmt = connection.createStatement()) {
			connection.setAutoCommit(false);
			for (String table : dropOrder) {
				//only delete the tables if they exist
				stmt.executeUpdate("IF OBJECT_ID('" + table + "', 'U') IS NOT NULL DROP TABLE " + table + ";");
			}
			connection.commit();
		}
		catch (SQLException e){
			try{
				connection.rollback();
				connection.setAutoCommit(true);
			}
			catch (SQLException r){
				System.err.println("At this point you just laugh ");
			}
		}
	}


	//return a map of the wards name to its generated primary keys
	public Map<String, Integer> populateWards(String csvFile) throws IOException, SQLException {
		Map<String, Integer> wardMap = new HashMap<>();
		try(
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertWard = connection.prepareStatement(
				"INSERT INTO Ward (WardE, WardF) VALUES (?, ?)",
				Statement.RETURN_GENERATED_KEYS
			);
		)
		{
			String line = br.readLine();

			//read line by line and extract the columns we need for wards 
			while ((line = br.readLine()) != null) {
				//only split on a comma that is not inside a quotation mark 
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

				//take out any accidental spaces in the data
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
				String wardE = parts[0];
				String wardF = parts[2];
		
				if (!wardMap.containsKey(wardE)) {
					insertWard.setString(1, wardE);
					insertWard.setString(2, wardF);
					insertWard.executeUpdate();
					
					//We need to keep track of the keys so that councillors can reference the correct ward
					ResultSet rs = insertWard.getGeneratedKeys();
					if (rs.next()) wardMap.put(wardE, rs.getInt(1));
				}
			}
		}
		return wardMap;
	}


	//Virtually identical procedure to populate wards
	/*
	could have combined them into one function but 
	seperated them for readability and atomicity
	*/
	public Map<String, Integer> populateCouncillors(String csvFile, Map<String, Integer> wardMap) throws IOException, SQLException {
		Map<String, Integer> councillorMap = new HashMap<>();
		try(
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertCouncillor = connection.prepareStatement(
				"INSERT INTO Councillors (WID, Present, Name, Phone, Fax, WebsiteURL) VALUES (?, ?, ?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS
			);
		)
		{
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
		
				String wardE = parts[0];
				boolean present = parts[1].equalsIgnoreCase("true");
				String name = parts[4];
				String phone = parts[7];
				String fax = parts[8];
				String website = parts[9];
		
				//just in case we got bad data
				int wid = wardMap.getOrDefault(wardE, -1);
				if (wid == -1) 
				throw new IllegalStateException("Ward not found for councillor '" + name + "' in ward '" + wardE + "'");
				
		
				insertCouncillor.setInt(1, wid);
				insertCouncillor.setBoolean(2, present);
				insertCouncillor.setString(3, name);
				insertCouncillor.setString(4, phone);
				insertCouncillor.setString(5, fax);
				insertCouncillor.setString(6, website);
				insertCouncillor.executeUpdate();
		
				ResultSet rs = insertCouncillor.getGeneratedKeys();
				if (rs.next()) {
					//System.out.println(name + " " + rs.getInt(1));
					councillorMap.put(name.toLowerCase(), rs.getInt(1));
				}
			}
		}//try
		return councillorMap;
	}

	public void populateRepresentsTable(String csvFile, Map<String, Integer> councillorMap,
										Map<String, Integer> wardMap) throws IOException, SQLException 
	{
		Set<String> seenKeys = new HashSet<>();
		try(
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertRepresents = connection.prepareStatement(
				"INSERT INTO Represents (CID, WID) VALUES (?, ?)")
		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

				String councillorName = parts[4];
				String wardName = parts[0];

				Integer cid = councillorMap.get(councillorName.toLowerCase());
				Integer wid = wardMap.get(wardName);
				String key = Integer.toString(cid) + "|" + Integer.toString(wid);

				if (cid == null || wid == null) {
					System.err.println("Skipping row — missing CID or WID: " + councillorName + " | " + wardName);
					System.exit(0);
				}

				if(!seenKeys.contains(key)){
					insertRepresents.setInt(1, cid);
					insertRepresents.setInt(2, wid);
					insertRepresents.addBatch();
					seenKeys.add(key);
				}
			}
			insertRepresents.executeBatch();
		}
	}
	
	public void populateCouncilNeighbourhoods(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
		Set<String> seenKeys = new HashSet<>();
		try(
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertNeighbourhood = connection.prepareStatement(
				"INSERT INTO CouncilNeighbourhoods (CID, Area) VALUES (?, ?)"
			);
	
		)
		{
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
		

				String name = parts[4];

				//we have to check if neighbourhoods exist because the mayors have none 
				String neighbourhoods = parts.length > 10 ? parts[10].replaceAll("^\"|\"$" , "").trim() : "";
				Integer cid = councillorMap.get(name.toLowerCase());
			

				//some lines dont have neighbourhoods 
				//probably should throw an exception here if there's no matching cid
				if (cid == null) {
					System.out.println("Missing CID for: '" + name + "'");
				}
				if (cid == null || neighbourhoods.isEmpty()){
					//System.out.println("neighbourhood empty for cid: " + cid);
					continue;
				}
				else{
		
				for (String area : neighbourhoods.split(",")) {
					//System.out.println(Integer.toString(cid) + " " +area);
					area = area.trim();
					String key = cid + "|" + area;
					if (!area.isEmpty() && !seenKeys.contains(key)) {
						insertNeighbourhood.setInt(1, cid);
						insertNeighbourhood.setString(2, area);
						insertNeighbourhood.addBatch();
					}
					seenKeys.add(key);
				}
				insertNeighbourhood.executeBatch();
			}
			}
		}//try 
	}

	public void populateElections(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertElection = connection.prepareStatement(
				"INSERT INTO Election (ElectionDate, CID, Type, Position) VALUES (?, ?, ?, ?)")
		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

				LocalDate parsDate = LocalDate.parse(parts[0], inputFormat);
				String correctFormatDate = parsDate.format(outputFormat);
	
				String type = parts[1];
				String candidate = parts[3];
				String position = parts[4];
				//int votes = Integer.parseInt(parts[5]);
				Integer cid = councillorMap.get(candidate.toLowerCase());
				if (cid == null) {
					continue; //skip any councillor thats not in our table
				}
	
				//System.out.println(parts[3]);
				insertElection.setString(1, correctFormatDate);
				insertElection.setInt(2, cid);
				insertElection.setString(3, type);
				insertElection.setString(4, position);
				//insertElection.setInt(5, votes);
				insertElection.addBatch();
			}
			insertElection.executeBatch();
		}
	}

	
	public void populateYearsServed(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertYears = connection.prepareStatement(
				"INSERT INTO YearsServed (CID, Year) VALUES (?, ?)")
		) {
			String line = br.readLine(); // skip header
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String candidate = parts[4];
				String year = parts[3];
				Integer cid = councillorMap.get(candidate.toLowerCase());
				if (cid == null) {

				}
	
				insertYears.setInt(1, cid);
				insertYears.setInt(2, Integer.parseInt(year));
				insertYears.addBatch();
			}
			insertYears.executeBatch();
		}
	}


	public void populateParticipates(String csvFile) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertParticipates = connection.prepareStatement(
				"INSERT INTO Participates (Participant, ElectionDate, Votes) VALUES (?, ?, ?)")
		) {
			String line = br.readLine(); 

			Set<String> candidatekeys = new HashSet<>();

			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String candidate = parts[3];
				int votes = Integer.parseInt(parts[5]);				
				LocalDate parsDate = LocalDate.parse(parts[0], inputFormat);
				String correctFormatDate = parsDate.format(outputFormat);
				//only add unique pairs of councillor,date
				String key = candidate + "|" + correctFormatDate;
				if(candidatekeys.contains(key))
					continue;
				insertParticipates.setString(1, candidate);
				insertParticipates.setString(2, correctFormatDate);
				insertParticipates.setInt(3, votes);
				insertParticipates.addBatch();
				candidatekeys.add(key);
			}
			insertParticipates.executeBatch();
		}
	}

	/*
	 * We have two third party functions because thirdparty entities come from separate csv's
	 */
	public void populateThirdPartyVendors(String csvFile, Map<String, Integer> vendorMap) throws IOException, SQLException {

		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertVendor = connection.prepareStatement(
				"INSERT INTO ThirdParty (Name, Address, Phone, Email, isBusiness, isVendor) VALUES (?, ?, ?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS
			)
		) {
			String line = br.readLine(); 
	
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String vendor = parts[3];
				//check if that vendors already in the table or there isnt one 
				if (vendor.isEmpty() || vendorMap.containsKey(vendor)) continue;
	
				insertVendor.setString(1, vendor); // Name
				insertVendor.setString(2, null);// Address
				insertVendor.setString(3, null);// Phone
				insertVendor.setString(4, null); // Email
				insertVendor.setBoolean(5, false);// isBusiness
				insertVendor.setBoolean(6, true);// isVendor
				insertVendor.executeUpdate();
	
				ResultSet rs = insertVendor.getGeneratedKeys();
				if (rs.next()) {
					vendorMap.put(vendor, rs.getInt(1));
				}
			}
		}
	}

	/*
	 * Third parties that are businesses
	 */
	public void populateThirdPartyBusinesses(String csvFile, Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
        try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
            PreparedStatement insertThirdParty = connection.prepareStatement(
                "INSERT INTO ThirdParty (Name, Address, Phone, Email, isBusiness, isVendor) VALUES (?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
		){
			String line = br.readLine(); 
			line = br.readLine();
			while(line != null){
				while (line != null && line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length < 15) {
					String next = br.readLine();
					if(next == null) break;
					line +=  " " + next;
				}
				
                String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                for (int i = 0; i < parts.length; i++) {
					parts[i] = parts[i].trim();
				}
                String businessName = parts[1];

				//again check if the third party entity already exists or theres no name
                if (businessName.isEmpty() || thirdPartyMap.containsKey(businessName)) {
					line = br.readLine();
					continue;
				}
                String address = parts[2];
                String phone = parts[3];
                String email = parts[4];
				String clientBusiness = parts[5];

                insertThirdParty.setString(1, businessName);
                insertThirdParty.setString(2, address);
                insertThirdParty.setString(3, phone);
                insertThirdParty.setString(4, email);
                insertThirdParty.setBoolean(5, true);  // isBusiness
                insertThirdParty.setBoolean(6, false); // isVendor
                insertThirdParty.executeUpdate();

                ResultSet rs = insertThirdParty.getGeneratedKeys();
                if (rs.next()) {
                    int tid = rs.getInt(1);
					if(clientBusiness != null)
                    	thirdPartyMap.put(businessName, tid);
                }
				line = br.readLine();
            }
        }//try
	}


	public void populateThirdPartyGift(String csvFile, Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertGift = connection.prepareStatement(
				"INSERT INTO ThirdParty (Name, Address, Phone, Email, isBusiness, isVendor) VALUES (?, ?, ?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS)

		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String name = parts[6];

				insertGift.setString(1, name);
				insertGift.setNull(2, Types.VARCHAR);
				insertGift.setNull(3, Types.VARCHAR);
				insertGift.setNull(4, Types.VARCHAR);
				insertGift.setBoolean(5, false);
				insertGift.setBoolean(6, false);
				insertGift.executeUpdate();

				ResultSet rs = insertGift.getGeneratedKeys();
				if (rs.next()) {
					int tid = rs.getInt(1);
					if(name != null)
						thirdPartyMap.put(name, tid);
				}
			}
		}
	}


	//populate business owners 
    public Map<String, Integer> populateBusinessOwners(String csvFile, Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
		Map<String, Integer> ownerMap = new HashMap<>();
		Set<String> candidatekeys = new HashSet<>();
        try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
            PreparedStatement insertOwner = connection.prepareStatement(
                    "INSERT INTO BusinessOwner (Name, TID) VALUES (?, ?)",
					Statement.RETURN_GENERATED_KEYS
            );
		){
			String line = br.readLine(); 
			line = br.readLine();
			while(line != null){
				while (line != null && line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length < 15) {
					//System.out.println(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length);
					String next = br.readLine();
					if(next == null) break;
					line += " " +next;
				}
				//line = line.replaceAll("\n", " ");
                String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

                String name = parts[0];
				String businessName = parts[1];
				//System.out.println(name);
                //String clientOwner = parts[6];
				Integer tid = thirdPartyMap.get(businessName);
				//System.out.println(tid);
				String key = name + "|" + tid;
                if (name.isEmpty() || tid == null || candidatekeys.contains(key)) {
					line = br.readLine();
					continue;
				}
				candidatekeys.add(name + "|" + tid);
             
                insertOwner.setString(1, name);
                insertOwner.setInt(2, tid);
                insertOwner.executeUpdate();

				ResultSet rs = insertOwner.getGeneratedKeys();
				if(rs.next()) ownerMap.put(name, rs.getInt(1));
				line = br.readLine();
            }
		}//try
		return ownerMap;
    }

	//populate buys from 
	public void populateBuysFrom(String csvFile, Map<String, Integer> councillorMap, Map<String, Integer> vendorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertPurchase = connection.prepareStatement(
				"INSERT INTO BuysFrom (CID, Date, Vendor, ExpenseType, Description, Account, Amount, Department) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
			)
		) {
			String line = br.readLine();
	
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String councillorName = parts[1];
				String vendor = parts[3];
	
				Integer cid = councillorMap.get(councillorName.toLowerCase());
				Integer tid = vendorMap.get(vendor);
	
				//need to throw exception here 
				if (cid == null || tid == null) continue;
	
				insertPurchase.setInt(1, cid); // CID
				insertPurchase.setString(2, parts[2]); // Date
				insertPurchase.setInt(3, tid); // Vendor (TID)
				insertPurchase.setString(4, parts[4]); // ExpenseType
				insertPurchase.setString(5, parts[5]); // Description
				insertPurchase.setString(6, parts[6]); // Account
				insertPurchase.setBigDecimal(7, new BigDecimal(parts[7])); // Amount
				//these look like they're all empty in our csv so not sure what were doing with this
				insertPurchase.setString(8, parts.length > 8 ? parts[8] : null); // Department
				insertPurchase.addBatch();
			}

			insertPurchase.executeBatch();
		}
	}

	public void populateGiftTable( String csvFile) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertGift = connection.prepareStatement(
				"INSERT INTO Gift (GID, Description, Value) VALUES (?, ?, ?)")
		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				int gid = Integer.parseInt(parts[0]);
				String description = parts[8];
				String valueStr = parts[11];
				BigDecimal value = valueStr.isEmpty() ? null : new BigDecimal(valueStr);
	
				insertGift.setInt(1, gid);
				insertGift.setString(2, description);
				if (value == null)
					insertGift.setNull(3, Types.DECIMAL);
				else
					insertGift.setBigDecimal(3, value);
	
				insertGift.addBatch();
			}
			insertGift.executeBatch();
		}
	}


	public void populateGifts(String csvFile,
                            	Map<String, Integer> councillorMap,
                            	Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertGifts = connection.prepareStatement(
				"INSERT INTO Gifts (GID, DateRecorded, Councillor, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

				int gid = Integer.parseInt(parts[0]);
				String dateRecorded = parts[1];
				String councillorName = parts[2];
				boolean self = parts[3].equalsIgnoreCase("yes");
				boolean dependent = parts[4].equalsIgnoreCase("yes");
				boolean staff = parts[5].equalsIgnoreCase("yes");
				String sourceName = parts[6];
				String dateGifted = parts[7];
				String reason = parts[9];
				String intent = parts[10];

				Integer cid = councillorMap.get(councillorName.toLowerCase());
				Integer tid = thirdPartyMap.get(sourceName);

				//throw an exception here
				if (cid == null || tid == null) {
					continue; 
				}

				insertGifts.setInt(1, gid);
				insertGifts.setString(2, dateRecorded);
				insertGifts.setInt(3, cid);
				insertGifts.setBoolean(4, self);
				insertGifts.setBoolean(5, dependent);
				insertGifts.setBoolean(6, staff);
				insertGifts.setInt(7, tid);
				insertGifts.setString(8, dateGifted);
				insertGifts.setString(9, reason);
				insertGifts.setString(10, intent);
				insertGifts.addBatch();
			}
			insertGifts.executeBatch();
			}
	}

	// Function to populate Owns table
	public void populateOwns(String csvFile, Map<String, Integer> ownerMap, Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertOwns = connection.prepareStatement(
				"INSERT INTO Owns (OwnerID, Business) VALUES (?, ?)"
			)
		) { 
			Set<String> seenKeys = new HashSet<>();
			String line = br.readLine(); 
			line = br.readLine();
			while(line != null){
				while (line != null && line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length < 15) {
					String next = br.readLine();
					if(next == null) break;
					line += " " +next;
				}

				line = line.replaceAll("\n", " ");
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

				/* 
				String ownerName = parts[6]; // Client Business Owner
				String clientBusiness = parts[5]; // Client Business Name
				if (ownerName.isEmpty() || clientBusiness.isEmpty()) {
					line = br.readLine();
					continue;
				}
				*/

				Integer ownerID = ownerMap.get(parts[0]);
				Integer businessID = thirdPartyMap.get(parts[1]);
				if (ownerID == null || businessID == null) {
					line = br.readLine();
					continue;
				}

				String key = ownerID + "|" + businessID;
				if (seenKeys.contains(key)){
					line = br.readLine();
					continue;
				}
				seenKeys.add(key);

				insertOwns.setInt(1, ownerID);
				insertOwns.setInt(2, businessID);
				insertOwns.addBatch();
				line = br.readLine();
			}
			insertOwns.executeBatch();
		}
	}

	public void populateLobbies(String csvFile, Map<String, Integer> ownerMap, Map<String, Integer> businessMap, Map<String, Integer> councillorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertLobbies = connection.prepareStatement(
				"INSERT INTO Lobbies (Owner, Business, ClientBusiness, CID, LobbyDate, Subject, IntendedOutcome) VALUES (?, ?, ?, ?, ?, ?, ?)"
			)
		) {
			String line = br.readLine(); 
			line = br.readLine();
			while(line != null){
				while (line != null && line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length < 15) {
					String next = br.readLine();
					if(next == null) break;
					line += " " +next;
				}
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String ownerName = parts[0];
				String businessName = parts[1];
				String clientBusinessName = parts[5];
				String personLobbied = parts[10];
				String date = parts[12];
				String subject = parts[13];
				String outcome = parts[14];
	
				Integer ownerID = ownerMap.get(ownerName);
				Integer businessID = businessMap.get(businessName);
				//Integer clientID = clientBusinessName.isEmpty() ? null : businessMap.get(clientBusinessName);
				Integer cid = councillorMap.get(personLobbied.toLowerCase());
	
				if (ownerID == null || businessID == null || cid == null || date.isEmpty()){
					line = br.readLine();
					continue;
				}
	
				insertLobbies.setInt(1, ownerID);
				insertLobbies.setInt(2, businessID);
				if (clientBusinessName != null) insertLobbies.setString(3, clientBusinessName);
				else insertLobbies.setNull(3, Types.VARCHAR);
	
				insertLobbies.setInt(4, cid);
				insertLobbies.setString(5, date);
				insertLobbies.setString(6, subject);
				insertLobbies.setString(7, outcome);
				insertLobbies.addBatch();
				line = br.readLine();
			}
			insertLobbies.executeBatch();
		}
	}

	//Full table queries
	public void allWards()
	{
		try {
			String sqlMessage = "SELECT WID, WardE, WardF FROM Ward;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-40s |%-40s |", "WID", "Ward English", "Ward French"));
			while(resultSet.next()){
				System.out.println(String.format("%-5s |%-40s |%-40s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void allCouncillors()
	{
		try
		{
			String sqlMessage = "SELECT CID, WID, present, name, phone, fax, websiteurl from councillors;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-5s |%-8s |%-20s |%-12s |%-12s |%-40s |","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL"));
			while(resultSet.next()){
				System.out.println(String.format("%-5s |%-5s |%-8s |%-20s |%-12s |%-12s |%-40s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void allNeighbourhoods()
	{
		try
		{
			String sqlMessage = "SELECT DISTINCT area FROM CouncilNeighbourhoods;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-30s |", "Area"));
			while(resultSet.next()){
				System.out.println(String.format("%-30s |", resultSet.getString(1)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void allThirdParty()
	{
		try
		{
			String sqlMessage = "SELECT TID, Name, Address, Phone, Email FROM ThirdParty;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-40s |%-20s |%-12s |%-35s |","TID", "Name", "Address", "Phone", "Email"));
			while(resultSet.next()){
				System.out.println(String.format("%-5s |%-40s |%-20s |%-12s |%-35s |", resultSet.getString(1), truncateString(resultSet.getString(2), 35), truncateString(resultSet.getString(3), 15), resultSet.getString(4), resultSet.getString(5)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void allBusinessOwners()
	{
		try
		{
			String sqlMessage = "select ownerid, name, tid from BusinessOwner;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-10s |%-40s |%-5s |","OwnerID", "Name", "TID"));
			while(resultSet.next()){
				System.out.println(String.format("%-10s |%-40s |%-5s |", resultSet.getString(1), truncateString(resultSet.getString(2), 35), resultSet.getString(3)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	//Queries
	/*1*****************/
	public void searchWard(String ward) {
		try {
			String sqlMessage = "SELECT WID, WardE, WardF FROM Ward WHERE Ward.WardE LIKE ? OR Ward.WardF LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%"+ward+"%");
			statement.setString(2, "%"+ward+"%");
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-40s |%-40s |","WID", "Ward English", "Ward French"));
			while(resultSet.next()){
				System.out.println(String.format("%-5s |%-40s |%-40s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void searchCouncillor(String name)
	{
		try
		{
			String sqlMessage = "SELECT name, Councillors.CID, Present, Phone, Fax, WebsiteURL, Year FROM Councillors JOIN YearsServed ON Councillors.CID=YearsServed.CID WHERE Councillors.Name LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%" + name + "%");
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-20s |%-5s |%-8s |%-12s |%-12s |%-40s |%-10s |","Councillor", "CID", "Present", "Phone", "Fax", "WebsiteURL", "Year"));
			while(resultSet.next()){
				System.out.println(String.format("%-20s |%-5s |%-8s |%-12s |%-12s |%-40s |%-10s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	public void councillorByNBH(String nbhString)
	{
		try 
		{
			String sqlMessage = "SELECT Councillors.Name, WID, CouncilNeighbourhoods.Area, Phone, Fax FROM Councillors JOIN CouncilNeighbourhoods ON Councillors.CID=CouncilNeighbourhoods.CID WHERE CouncilNeighbourhoods.Area LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%"+nbhString+"%");
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-20s |%-5s |%-40s |%-12s |%-12s |", "Councillor", "WID", "Neighbourhood Name", "Phone", "Fax"));
			while(resultSet.next()){
				System.out.println(String.format("%-20s |%-5s |%-40s |%-12s |%-12s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5)));
			}
		} 
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void searchCouncilYear(String year)
	{
		try
		{
			String sqlMessage = "SELECT name, wid, present, phone, fax, websiteurl FROM Councillors JOIN YearsServed ON Councillors.CID=YearsServed.CID WHERE YearsServed.Year=?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, year);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-20s |%-5s |%-8s |%-12s |%-12s |%-40s |", "Councillor", "WID", "Present", "Phone", "Fax", "WebsiteURL"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s |%-5s |%-8s |%-12s |%-12s |%-40s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void searchBusinessName(String name)
	{
		try
		{
			String sqlMessage = "SELECT Name, Address, Phone, Email FROM ThirdParty WHERE ThirdParty.isBusiness='TRUE' AND ThirdParty.Name LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%"+name+"%");
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-40s |%-20s |%-12s |%-35s |","Business", "Address", "Phone", "Email"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-40s |%-20s |%-12s |%-35s |", resultSet.getString(1), truncateString(resultSet.getString(2), 35), truncateString(resultSet.getString(3), 15), resultSet.getString(4)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void businessByOwner(String owner)
	{
		try
		{
			String sqlMessage = "SELECT ThirdParty.name, BusinessOwner.Name, Address, Phone, Email FROM ThirdParty JOIN Owns ON ThirdParty.TID=Owns.Business JOIN BusinessOwner ON Owns.OwnerID=BusinessOwner.OwnerID WHERE BusinessOwner.Name LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%"+owner+"%");
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-35s |%-35s |%-20s |%-12s |%-35s |","Business", "Owner", "Address", "Phone", "Email"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-35s |%-35s |%-20s |%-12s |%-35s |", truncateString(resultSet.getString(1), 30), truncateString(resultSet.getString(2), 30), truncateString(resultSet.getString(3), 15), resultSet.getString(4), resultSet.getString(5)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void expensesByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT BuysFrom.PurchaseID as PurchaseID, BuysFrom.Date as Date,"
					+ "ThirdParty.TID as Source, BuysFrom.ExpenseType as Type, BuysFrom.Account as Account,"
					+ "BuysFrom.Amount as Amount, BuysFrom.Description as Description,"
					+ "BuysFrom.Department as Department "
					+ "FROM Councillors JOIN BuysFrom ON Councillors.CID=BuysFrom.CID JOIN ThirdParty ON BuysFrom.Vendor=ThirdParty.TID "
					+ "WHERE Councillors.CID=? ORDER BY Date DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-15s |%-12s |%-8s |%-12s |%-15s |%-8s |%-40s |%-20s |","PurchaseID", "Date", "Source", "Type", "Account", "Amount", "Description", "Department"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-15s |%-12s |%-8s |%-12s |%-15s |%-8s |%-40s |%-20s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), truncateString(resultSet.getString(5), 12), resultSet.getString(6), truncateString(resultSet.getString(7), 35), resultSet.getString(8)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void giftsByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT Gift.description, DateRecorded, Councillors.name, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent, Gift.Value"
					+ " FROM Councillors JOIN Gifts ON Councillors.CID=Gifts.Councillor JOIN Gift ON Gifts.GID=Gift.GID JOIN ThirdParty ON Gifts.Source=ThirdParty.TID"
					+ " WHERE Councillors.CID=? ORDER BY Gifts.DateGifted DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-20s |%-12s |%-20s |%-10s |%-10s |%-6s |%-20s |%-12s |%-10s |%-10s |%-6s |","Gift", "DateRecorded", "Councillor", "Recipient", "Dependent", "Staff", "Source", "DateGifted", "Reason", "Intent", "Value"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s |%-12s |%-20s |%-10s |%-10s |%-6s |%-20s |%-12s |%-10s |%-10s |%-6s |", truncateString(resultSet.getString(1), 15), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), truncateString(resultSet.getString(7), 15), resultSet.getString(8), truncateString(resultSet.getString(9), 7), truncateString(resultSet.getString(10), 15), resultSet.getString(11)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void lobbiesByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT LID, Owner, Business, ClientBusiness, Councillors.name, LobbyDate, Subject, IntendedOutcome"
					+ " FROM Councillors JOIN Lobbies ON Councillors.CID=Lobbies.CID"
					+ " WHERE Councillors.CID=? ORDER BY Lobbies.LobbyDate DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-5s |%-10s |%-16s |%-5s |%-12s |%-20s |%-20s |","LID", "Owner", "Business", "ClientBusiness", "Councillor", "Date", "Subject", "IntendedOutcome"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-5s |%-10s |%-16s |%-5s |%-12s |%-20s |%-20s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), truncateString(resultSet.getString(4), 12), resultSet.getString(5), resultSet.getString(6), truncateString(resultSet.getString(7), 15), truncateString(resultSet.getString(8), 15)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void giftsByDate(String cid, String date, String date2)
	{
		try
		{
			String sqlMessage = "SELECT GID, DateRecorded, Councillor, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent FROM Gifts WHERE DateGifted BETWEEN ? AND ? AND (? IS NULL OR Councillor = ?);";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, cid);
			statement.setString(4, cid);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-12s |%-5s |%-10s |%-10s |%-6s |%-30s |%-12s |%-20s |%-20s |","GID", "DateRecorded", "CID", "Recipient", "Dependent", "Staff", "Source", "DateGifted", "Reason", "Intent"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-12s |%-5s |%-10s |%-10s |%-6s |%-30s |%-12s |%-20s |%-20s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), truncateString(resultSet.getString(7), 26), resultSet.getString(8), truncateString(resultSet.getString(9), 15), truncateString(resultSet.getString(10), 15)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void expensesByDate(String cid, String date, String date2)
	{
		try
		{
			String sqlMessage = "SELECT PurchaseID, CID, Date, Vendor, ExpenseType, Description, Account, Amount, Department FROM BuysFrom WHERE Date BETWEEN ? AND ? AND (? IS NULL OR CID = ?);";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, cid);
			statement.setString(4, cid);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-11s |%-5s |%-12s |%-20s |%-20s |%-20s |%-20s |%-8s |%-20s |", "PurchaseID", "CID", "Date", "Vendor", "ExpenseType", "Description", "Account", "Amount", "Department"));
			while(resultSet.next())
			{
				System.out.println(String.format("%11s |%-5s |%-12s |%-20s |%-20s |%-20s |%-20s |%-8s |%-20s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), truncateString(resultSet.getString(6), 15), truncateString(resultSet.getString(7), 15), resultSet.getString(8), truncateString(resultSet.getString(9), 15)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void perfectCouncillorsByDate(String date, String date2)
	{
		try
		{
			String sqlMessage = "WITH CLobbies AS "
			+"(SELECT Lobbies.CID as LCID FROM Lobbies WHERE Lobbies.LobbyDate BETWEEN ? AND ?), "
			+"CGifts AS "
			+"(SELECT Gifts.Councillor as GC FROM Gifts WHERE DateGifted BETWEEN ? AND ?), "
			+"GiftLobby AS ( "
			+"(SELECT CGifts.GC as CID FROM CGifts) "
			+"UNION "
			+"(SELECT CLobbies.LCID as CID FROM CLobbies) "
			+") "
			+"SELECT Councillors.name "
			+"FROM Councillors "
			+"EXCEPT "
			+"SELECT Councillors.name "
			+"FROM Councillors JOIN GiftLobby ON Councillors.CID=GiftLobby.CID;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, date);
			statement.setString(4, date2);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |", "Councillor"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |", resultSet.getString(1)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void totalPriceGifts(String councillor)
	{
		try
		{
			String sqlMessage = "select gifts.Councillor, sum(gift.value) as totalValue" 
								+ " from gifts join Gift on gifts.gid = gift.gid where gifts.Councillor = ? group by gifts.Councillor;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, councillor);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-20s |","CID", "TotalValue"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-20s |", resultSet.getString(1), resultSet.getString(2)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void totalPriceExpenses(String councillor)
	{
		try
		{
			String sqlMessage = "select councillors.cid, sum(BuysFrom.Amount) as total" 
								+ " from Councillors join BuysFrom on Councillors.CID=BuysFrom.CID where Councillors.CID = ?  group by councillors.cid;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, councillor);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-20s |","CID", "TotalValue"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-20s |", resultSet.getString(1), resultSet.getString(2)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void bigSpenders()
	{
		try
		{
			String sqlMessage = "select TOP 10 Councillors.CID, Councillors.name, sum(BuysFrom.Amount) as total" 
								+ " from BuysFrom join Councillors on BuysFrom.CID=Councillors.CID group by Councillors.CID, Councillors.Name order by total DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			//statement.setString(1, gifter);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-20s |%-20s |", "CID", "Name", "TotalValue"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-20s |%-20s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void electionGifts()
	{
		try
		{
			String sqlMessage = "SELECT TOP 10 Gifts.GID, Gifts.Councillor, Gifts.DateGifted, min(abs(DATEDIFF(DAY, Gifts.DateGifted, Election.ElectionDate))) AS diff FROM Gift JOIN Gifts ON Gift.GID=Gifts.GID JOIN Election ON Gifts.Councillor = Election.CID GROUP BY Gifts.GID, Gifts.Councillor, Gifts.DateGifted ORDER BY diff ASC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-5s |%-12s |%-12s |", "GID", "CID", "Date Gifted", "Difference"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-5s |%-12s |%-12s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void giftGap()
	{
		try
		{
			String sqlMessage = "SELECT TOP 10 Gifts.GID, Gifts.Councillor, Gifts.DateGifted, Gifts.DateRecorded, DATEDIFF(DAY, Gifts.DateGifted, Gifts.DateRecorded) AS diff FROM Gifts GROUP BY Gifts.GID, Gifts.Councillor, Gifts.DateGifted, Gifts.DateRecorded ORDER BY diff DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-5s |%-12s |%-12s |", "GID", "CID", "Date Gifted", "Difference"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-5s |%-12s |%-12s |", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void bigGivers()
	{
		try
		{
			String sqlMessage = "SELECT TOP 10 t.TID, t.Name AS ThirdPartyName, COUNT(DISTINCT g.GID) AS total_gifts, COUNT(DISTINCT l.LobbyDate) AS total_lobbies FROM ThirdParty t LEFT JOIN Gifts g ON t.TID = g.Source LEFT JOIN Lobbies l ON t.TID = l.Business GROUP BY t.TID, t.Name HAVING COUNT(g.GID) > 0 OR COUNT(l.LobbyDate) > 0 ORDER BY (COUNT(DISTINCT g.GID) + COUNT(DISTINCT l.LobbyDate)) DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			printHeader(String.format("%-5s |%-40s |%-15s |%-15s |", "TID", "Third Party Name", "Total Gifts", "Total Lobbies"));
			while(resultSet.next())
			{
				System.out.println(String.format("%-5s |%-40s |%-15s |%-15s |", resultSet.getString(1), truncateString(resultSet.getString(2), 35), resultSet.getString(3), resultSet.getString(4)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	private void printHeader(String cols) 
	{
		System.out.println(cols);
		String dash = "";
		for(int i = 0; i < cols.length(); i++)
		{
			dash += "-";
		}
		System.out.println(dash);
	}

	private String truncateString(String text, int length)
	{
		if (text == null) return "";

		if(text.length() <= length) return text;

		else return text.substring(0,length) + "...";
	}
	
}