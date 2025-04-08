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
				else if(mode == 1)//Browse Mode. All "lookup" queries go here
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
				}
				else if(mode == 3)//Maintenance Mode. Delete and restore queries go here
				{
					if(parts[0].equals("1"))
					{
						mode = 1;
					}
					else if(parts[0].equals("2"))
					{
						mode = 2;
					}
					else if(parts[0].equals("deleteTables")) 
					{
						System.out.println("\nYou are about to delete all data. Type 'confirm' to procced");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("Attempting to delete data...");
							db.deleteTables();
						}
						else {
							System.out.println("Delete canceled.");
						}
					}
					else if(parts[0].equals("populateTables")) 
					{
						System.out.println("\nYou are about to populate the database. Type 'confirm' to procced");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("Attempting to populate database...");
							db.createTables();
						}
						else {
							System.out.println("Populate canceled.");
						}	
					}
				}
				
				else 
				{
					System.out.println("Type help for help");
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
		System.out.println("Welcome to the Winnipeg Council Transparency Database!");
		System.out.println("How to use:");
		System.out.println("-----------");
		System.out.println("Enter '1' for Browse Mode if you want to lookup data");
		System.out.println("Enter '2' for Search Mode if you want to search for relationships in data");
		System.out.println("Enter '3' for Maintenance Mode for clearing and re-populating the database");
		System.out.println("Enter 'help' at any time for detailed help, or 'q' to quit");
	}
	private static void printHelp(int mode) 
	{
		if(mode == 1)//Browse Mode
		{
			System.out.println("Browse Mode Commands:");
			System.out.println("w <name> :Lookup Ward data by <name>");
			System.out.println("-");
			System.out.println("c n <name> :Lookup Councillor data by <name>");
			System.out.println("c h <neighbourhoodName>:Lookup Councillor data by <neighbourhoodName>");
			System.out.println("c y <YYYY>:Lookup Councillor data by <YYYY> (year)");
			System.out.println("-");
			System.out.println("b n <name>:Lookup Business data by <name>");
			System.out.println("b own <ownerName>:Lookup Business data by <ownerName>");
			System.out.println("-----");
			System.out.println("Enter 2 to enter Search Mode, 3 for Maintenance Mode, or q to quit");
		}
		else if(mode == 2)//Search Mode
		{
			System.out.println("Search Mode Commands:");
			System.out.println("c e <CID>:Get Councillor expenses by <CID>");
			System.out.println("c g <CID>:Get Councillor gifts received by <CID>");
			System.out.println("c l <CID>:Get Councillor lobby activities by <CID>");
			System.out.println("-");
			System.out.println("r e <YYYYMMDD> <YYYYMMDD>:Show all expenses in a date range");
			System.out.println("r g <YYYYMMDD> <YYYYMMDD>:Show all gifts in a date range");
			System.out.println("-");
			System.out.println("n <YYYYMMDD> <YYYYMMDD>:Show councillors that have not participated in any lobby activities or received any gifts in a date range");
			System.out.println("-");
			System.out.println("v e <CID>:Get expendature total by <CID>");
			System.out.println("v g <CID>:Get total value of gifts received by <CID>");
			System.out.println("-");
			System.out.println("t e:Show Top 10 councillors that spend the most");
			System.out.println("-");
			System.out.println("s:Show Top 10 gifts that were given closest to an election");
			System.out.println("g:Show Top 10 gifts with the largest gap between date recorded and date received");
			System.out.println("l:Show Top 10 third-parties that have given gifts and/or preformed lobby activities");
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
	private static final String MAX_OUTPUT = "TOP 1000 ";

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
			System.out.println("Connection success");
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	//CREATE TABLES 
	public void createTables() {
		Statement stmt = null;
		try {
			// Turn off auto-commit to start a transaction.
			connection.setAutoCommit(false);
			
			stmt = connection.createStatement();
			
			// 1. Create Ward table
			stmt.executeUpdate(
				"CREATE TABLE Ward (" +
				"  WID INT PRIMARY KEY IDENTITY(1,1), " +
				"  WardE NVARCHAR(255), " +
				"  WardF NVARCHAR(255)" +
				");"
			);
			
			// 2. Create Councillors table (with foreign key to Ward)
			stmt.executeUpdate(
				"CREATE TABLE Councillors (" +
				"  CID INT PRIMARY KEY IDENTITY(1,1), " +
				"  WID INT, " +
				"  Present BIT, " +
				"  Name NVARCHAR(255), " +
				"  Phone NVARCHAR(50), " +
				"  Fax NVARCHAR(50), " +
				"  WebsiteURL NVARCHAR(255), " +
				"  FOREIGN KEY (WID) REFERENCES Ward(WID)" +
				");"
			);
			
			// 3. Create Election table
			stmt.executeUpdate(
				"CREATE TABLE Election (" +
				"  ElectionDate DATE, " +
				"  CID INT, " +
				"  Type NVARCHAR(100), " +
				"  Position NVARCHAR(100), " +
				"  Votes INT, " +
				"  PRIMARY KEY (ElectionDate, CID), " +
				"  FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// 4. Create YearsServed table
			stmt.executeUpdate(
				"CREATE TABLE YearsServed (" +
				"  CID INT, " +
				"  Year INT, " +
				"  PRIMARY KEY (CID, Year), " +
				"  FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// 5. Create CouncilNeighbourhoods table
			stmt.executeUpdate(
				"CREATE TABLE CouncilNeighbourhoods (" +
				"  CID INT, " +
				"  Area NVARCHAR(255), " +
				"  PRIMARY KEY (CID, Area), " +
				"  FOREIGN KEY (CID) REFERENCES Councillors(CID)" +
				");"
			);
			
			// 6. Create ThirdParty table
			stmt.executeUpdate(
				"CREATE TABLE ThirdParty (" +
				"  TID INT PRIMARY KEY IDENTITY(1,1), " +
				"  Name NVARCHAR(255), " +
				"  Address NVARCHAR(255), " +
				"  Phone NVARCHAR(50), " +
				"  Email NVARCHAR(255), " +
				"  isBusiness BIT, " +
				"  isVendor BIT" +
				");"
			);
			
			// 7. Create Gift table (for gift details)
			stmt.executeUpdate(
				"CREATE TABLE Gift (" +
				"  GID INT PRIMARY KEY IDENTITY(1,1), " +
				"  Description NVARCHAR(255), " +
				"  Value DECIMAL(10,2)" +
				");"
			);
			
			// 8. Create BusinessOwner table
			stmt.executeUpdate(
				"CREATE TABLE BusinessOwner (" +
				"  OwnerID INT PRIMARY KEY IDENTITY(1,1), " +
				"  Name NVARCHAR(255), " +
				"  TID INT, " +
				"  FOREIGN KEY (TID) REFERENCES ThirdParty(TID)" +
				");"
			);
			
			// 9. Create Represents table
			stmt.executeUpdate(
				"CREATE TABLE Represents (" +
				"  CID INT, " +
				"  WID INT, " +
				"  PRIMARY KEY (CID, WID), " +
				"  FOREIGN KEY (CID) REFERENCES Councillors(CID), " +
				"  FOREIGN KEY (WID) REFERENCES Ward(WID)" +
				");"
			);
			
			// 10. Create Participates table
			stmt.executeUpdate(
				"CREATE TABLE Participates (" +
				"  Councillor INT, " +
				"  ElectionDate DATE, " +
				"  ElectionCID INT, " +
				"  PRIMARY KEY (Councillor, ElectionDate, ElectionCID), " +
				"  FOREIGN KEY (Councillor) REFERENCES Councillors(CID), " +
				"  FOREIGN KEY (ElectionDate, ElectionCID) REFERENCES Election(ElectionDate, CID)" +
				");"
			);
			
			// 11. Create Gifts table (for gift transactions)
			stmt.executeUpdate(
				"CREATE TABLE Gifts (" +
				"  GID INT, " +
				"  DateRecorded DATE, " +
				"  Councillor INT, " +
				"  RecipientSelf NVARCHAR(10), " +
				"  RecipientDependent NVARCHAR(10), " +
				"  RecipientStaff NVARCHAR(10), " +
				"  Source INT, " +
				"  DateGifted DATE, " +
				"  Reason NVARCHAR(255), " +
				"  Intent NVARCHAR(255), " +
				"  PRIMARY KEY (GID, Councillor), " +
				"  FOREIGN KEY (GID) REFERENCES Gift(GID), " +
				"  FOREIGN KEY (Councillor) REFERENCES Councillors(CID), " +
				"  FOREIGN KEY (Source) REFERENCES ThirdParty(TID)" +
				");"
			);
			
			// 12. Create BuysFrom table
			stmt.executeUpdate(
				"CREATE TABLE BuysFrom (" +
				"  PurchaseID INT PRIMARY KEY IDENTITY(1,1), " +
				"  CID INT, " +
				"  Date DATE, " +
				"  Vendor INT, " +
				"  ExpenseType NVARCHAR(100), " +
				"  Description NVARCHAR(255), " +
				"  Account NVARCHAR(100), " +
				"  Amount DECIMAL(10,2), " +
				"  Department NVARCHAR(100), " +
				"  FOREIGN KEY (CID) REFERENCES Councillors(CID), " +
				"  FOREIGN KEY (Vendor) REFERENCES ThirdParty(TID)" +
				");"
			);
			
			// Commit the transaction if everything is successful.
			connection.commit();
			fillTables();
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
			populateParticipates(electionCsv, councillorMap);

			// 8. ThirdParty - Vendors from Expenses
			Map<String, Integer> thirdPartyMap = new HashMap<String, Integer>();
			populateThirdPartyVendors(lobbyistCsv, thirdPartyMap);

			// 9. ThirdParty - Businesses from Lobbyist Registry
			populateThirdPartyBusinesses(expensesCsv, thirdPartyMap);

			// 10. BuysFrom
			populateBuysFrom(expensesCsv, councillorMap, thirdPartyMap);

			// 11. Gifts (Gift + Gifts tables)
			populateGiftTable(giftsCsv);
			populateGifts(giftsCsv, councillorMap, thirdPartyMap);
			connection.commit();

			System.out.println("All tables populated successfully.");
		}
		catch (IOException | SQLException e) {
			try {
				connection.rollback(); // Rollback on failure
				System.err.println("Rolled back changes.");
				e.printStackTrace();
			} catch (SQLException ex) {
				System.err.println("Failed to rollback: " + ex.getMessage());

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
				System.out.println("Dropped table: " + table);
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
					councillorMap.put(name, rs.getInt(1));
				}
			}
		}//try
		return councillorMap;
	}

	public void populateRepresentsTable(String csvFile,
                                           Map<String, Integer> councillorMap,
                                           Map<String, Integer> wardMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertRepresents = connection.prepareStatement(
				"INSERT INTO Represents (CID, WID) VALUES (?, ?)")
		) {
			String line = br.readLine(); // skip header
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

				String councillorName = parts[4];
				String wardName = parts[0];

				Integer cid = councillorMap.get(councillorName);
				Integer wid = wardMap.get(wardName);

				if (cid == null || wid == null) {
					System.err.println("Skipping row — missing CID or WID: " + councillorName + " | " + wardName);
					System.exit(0);
				}

				insertRepresents.setInt(1, cid);
				insertRepresents.setInt(2, wid);
				insertRepresents.addBatch();
			}
			insertRepresents.executeBatch();
		}
	}
	
	public void populateCouncilNeighbourhoods(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
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
				String neighbourhoods = parts.length > 10 ? parts[10].replaceAll("^\"(.*)\"$", "$1") : "";
				Integer cid = councillorMap.get(name);

				//some lines dont have neighbourhoods 
				//probably should throw an exception here if there's no matching cid
				if (cid == null || neighbourhoods.isEmpty()) continue;
		
				for (String area : neighbourhoods.split(",")) {
					area = area.trim();
					if (!area.isEmpty()) {
						insertNeighbourhood.setInt(1, cid);
						insertNeighbourhood.setString(2, area);
						insertNeighbourhood.addBatch();
					}
				}
				insertNeighbourhood.executeBatch();
			}
		}//try 
	}

	public void populateElections(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertElection = connection.prepareStatement(
				"INSERT INTO Election (Date, CID, Type, Position, Votes) VALUES (?, ?, ?, ?, ?)")
		) {
			String line = br.readLine(); 
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String date = parts[0];
				String type = parts[1];
				String candidate = parts[3];
				String position = parts[4];
				int votes = Integer.parseInt(parts[5]);
	
				Integer cid = councillorMap.get(candidate);
				if (cid == null) {
					throw new IllegalStateException("Unknown candidate: " + candidate);
				}
	
				insertElection.setString(1, date);
				insertElection.setInt(2, cid);
				insertElection.setString(3, type);
				insertElection.setString(4, position);
				insertElection.setInt(5, votes);
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
	
				String candidate = parts[3];
				String date = parts[0];
				Integer cid = councillorMap.get(candidate);
				if (cid == null) {
					throw new IllegalStateException("Unknown candidate: " + candidate);
				}
	
				int year = Integer.parseInt(date.split(" ")[2]); 
				insertYears.setInt(1, cid);
				insertYears.setInt(2, year);
				insertYears.addBatch();
			}
			insertYears.executeBatch();
		}
	}

	public void populateParticipates(String csvFile, Map<String, Integer> councillorMap) throws IOException, SQLException {
		try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			PreparedStatement insertParticipates = connection.prepareStatement(
				"INSERT INTO Participates (Councillor, Election) VALUES (?, ?)")
		) {
			String line = br.readLine(); 

			Set<String> candidatekeys = new HashSet<>();

			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
	
				String candidate = parts[3];
				String date = parts[0];
				Integer cid = councillorMap.get(candidate);
				if (cid == null) {
					throw new IllegalStateException("Unknown candidate: " + candidate);
				}

				//only add unique pairs of councillor,date
				String key = cid + "|" + date;
				if(candidatekeys.contains(key)) continue;
				candidatekeys.add(key);
	
				insertParticipates.setInt(1, cid);
				insertParticipates.setString(2, date);
				insertParticipates.addBatch();
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
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

                String businessName = parts[1];

				//again check if the third party entity already exists or theres no name
                if (businessName.isEmpty() || thirdPartyMap.containsKey(businessName)) continue;

                String address = parts[2];
                String phone = parts[3];
                String email = parts[4];

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
                    thirdPartyMap.put(businessName, tid);
                }
            }
        }//try
		
    }

	//populate business owners 
    public void populateBusinessOwners(String csvFile, Map<String, Integer> thirdPartyMap) throws IOException, SQLException {
        try (
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
            PreparedStatement insertOwner = connection.prepareStatement(
                    "INSERT INTO BusinessOwner (Name, TID) VALUES (?, ?)"
            );
		){
			String line = br.readLine(); 
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

                String clientBusiness = parts[5];
                String clientOwner = parts[6];

                if (clientBusiness.isEmpty() || clientOwner.isEmpty()) continue;
                Integer tid = thirdPartyMap.get(clientBusiness);
                if (tid == null) continue;

                insertOwner.setString(1, clientOwner);
                insertOwner.setInt(2, tid);
                insertOwner.addBatch();
            }
            insertOwner.executeBatch();
		}//try
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
	
				Integer cid = councillorMap.get(councillorName);
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

	public void populateGiftTable(String csvFile) throws IOException, SQLException {
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

            Integer cid = councillorMap.get(councillorName);
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
	
	//Queries
	/*1*****************/
	public void searchWard(String ward) {
		try {
			String sqlMessage = "SELECT WID, WardE, WardF FROM Ward WHERE Ward.WardE LIKE %?% OR Ward.WardF LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, ward);
			statement.setString(2, ward);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-10d\t|\t%-20s\t|\t%-20s\t|","WID", "WardE", "WardF"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-10d\t|\t%-20s\t|\t%-20s\t|", truncateString(resultSet.getString(1),30), truncateString(resultSet.getString(2), 10), truncateString(resultSet.getString(3), 10)));
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void searchCouncillor(String n)
	{
		try
		{
			String sqlMessage = "SELECT CID, WID, present, name, phone, fax, websiteurl, YearsServed, Ward FROM Councillors NATURAL JOIN YearsServed WHERE name LIKE ?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%" + n + "%");
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL", "YearsServed"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8)));
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
			String sqlMessage = "SELECT CID, WID, Present, Name, Phone, Fax, WebsiteURL FROM Councillors NATURAL JOIN CouncilNeighbourhoods WHERE CouncilNeighbourhoods.name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, nbhString);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
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
			String sqlMessage = "SELECT Councillors.* FROM Councillors NATURAL JOIN YearsServed WHERE YearsServed.Year=?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, year);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
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
			String sqlMessage = "SELECT TID, Name, Address, Phone, Email, isBusiness, isVendor FROM ThirdParty WHERE ThirdParty.isBusiness=TRUE AND ThirdParty.Name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","TID", "Name", "Address", "Phone", "Email", "isBusiness", "isVendor"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
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
			String sqlMessage = "SELECT TID, Name, Address, Phone, Email, isBusiness, isVendor FROM ThirdParty NATURAL JOIN Owns NATURAL JOIN BusinessOwner WHERE BusinessOwner.Name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, owner);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","TID", "Name", "Address", "Phone", "Email", "isBusiness", "isVendor"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	//2

	public void expensesByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT BuysFrom.PurchaseID as PurchaseID, BuysFrom.Date as Date,"
					+ "ThirdParty as From, BuysFrom.ExpenseType as Type, BuysFrom.Account as Account,"
					+ "BuysFrom.Amount as Amount, BuysFrom.Description as Description,"
					+ "BuysFrom.Department as Department"
					+ "FROM Councillors JOIN BuysFrom ON Councillors.CID=BuysFrom.CID JOIN ThirdParty ON BuysFrom.Vendor=ThirdParty.TID"
					+ "WHERE Councillors.CID=? ORDER BY Date DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","PurchaseID", "Date", "From", "Type", "Account", "Amount", "Description", "Department"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8)));
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
			String sqlMessage = "SELECT GID, DateRecorded, Councillor, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent, Gift.Description, Gift.Value"
					+ "FROM Councillors JOIN Gifts ON Councillors.CID=Gifts.CID JOIN Gift ON Gifts.GID=Gift.GID JOIN ThirdParty ON Gifts.Source=ThirdParty.TID"
					+ "WHERE Councillors.CID=? ORDER BY Gifts.DateGifted DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","GID", "DateRecorded", "Councillor", "RecipientSelf", "RecipientDependent", "RecipientStaff", "Source", "DateGifted", "Reason", "Intent", "Description", "Value"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8), resultSet.getString(9), resultSet.getString(10), resultSet.getString(11), resultSet.getString(12)));
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
			String sqlMessage = "SELECT LID, Owner, Business, ClientBusiness, CID, Date, Subject, IntendedOutcome"
					+ "FROM Councillors NATURAL JOIN Lobbies"
					+ "WHERE Councillors.CID=? Lobbies.Date DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","LID", "Owner", "Business", "ClientBusiness", "CID", "Date", "Subject", "IntendedOutcome"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8)));
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
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|","GID", "DateRecorded", "Councillor", "RecipientSelf", "RecipientDependent", "RecipientStaff", "Source", "DateGifted", "Reason", "Intent"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8), resultSet.getString(9), resultSet.getString(10)));
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
			String sqlMessage = "SELECT PurchaseID, CID, Date, Vendor, ExpenseType, Description, Account, Amount, Department FROM BuysFrom WHERE Date BETWEEN ? AND ? AND (? IS NULL OR Councillor = ?);";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, cid);
			statement.setString(4, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|", "PurchaseID", "CID", "Date", "Vendor", "ExpenseType", "Description", "Account", "Amount", "Department"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8), resultSet.getString(9)));
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
			String sqlMessage = "WITH CLobbies AS (SELECT * FROM Lobbies WHERE Date BETWEEN ? AND ?), CGifts AS (SELECT * FROM Gifts WHERE DateGifted BETWEEN ? AND ?) SELECT Councillors.CID, Councillors.name FROM Councillors WHERE NOT EXISTS (SELECT Councillors.CID, Councillors.name FROM Councillors NATURAL JOIN CLobbies NATURAL JOIN CGifts);";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, date);
			statement.setString(4, date2);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|", "CID", "Councillor"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2)));
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
			String sqlMessage = "select councillors.cid, councillors.name, sum(gift.value) as totalValue" 
								+ "from Councillors join Gifts on Councillors.CID = Gifts.Councillors join Gift on gifts.gid = gift.gid where councillors.name like %?% group by councillors.cid, councillors.name;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, councillor);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "Name", "TotalValue"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
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
								+ "from Councillors NATURAL JOIN BuysFrom  where Councillors.CID = ?  group by councillors.cid;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, councillor);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|","CID", "TotalValue"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2)));
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
			String sqlMessage = "select Councillors.CID, Councillors.name, sum(BuysFrom.Amount) as total" 
								+ "from BuysFrom NATURAL JOIN Councillors  group by CID, Name order by total DESC TOP 10;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			//statement.setString(1, gifter);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|", "CID", "Name", "TotalValue"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
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
			String sqlMessage = "SELECT Gifts.GID, Gifts.Councillor, Gifts.DateGifted, min(abs(Gifts.DateGifted - Election.Date)) AS diff FROM Gift NATURAL JOIN Gifts JOIN Election ON Gifts.Councillor = Election.CID GROUP BY Gifts.GID, Gifts.Councillor ORDER BY diff ASC TOP 10;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", "GiftID", "CouncillorID", "Date Gifted"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
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
			String sqlMessage = "SELECT Gifts.GID, Gifts.Councillor, Gifts.DateGifted, min(abs(Gifts.DateGifted - Gifts.DateRecorded)) AS diff FROM Gifts GROUP BY Gifts.GID, Gifts.Councillor ORDER BY diff ASC TOP 10;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", "GiftID", "CouncillorID", "Date Gifted"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
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
			String sqlMessage = "SELECT t.TID, t.Name AS ThirdPartyName,COUNT(DISTINCT g.GID) AS total_gifts, COUNT(DISTINCT l.Date) AS total_lobbies FROM ThirdParty t LEFT JOIN Gifts g ON t.TID = g.Source LEFT JOIN Lobbies l ON t.TID = l.Business GROUP BY t.TID, t.Name HAVING COUNT(g.GID) > 0 OR COUNT(l.Date) > 0 ORDER BY (COUNT(DISTINCT g.GID) + COUNT(DISTINCT l.Date)) DESC TOP 10;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", "GiftID", "CouncillorID", "Date Gifted"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	/*3*********************/

	/* 
	public void deleteTables() 
	{ 
    		try 
		{ 
    			File myObj = new File("./SQL_Files/DeleteTables.sql"); 
      			Scanner myReader = new Scanner(myObj); 
      			while (myReader.hasNextLine()) 
			{ 
        			String data = myReader.nextLine(); 
				try{
					PreparedStatement statement = connection.prepareStatement(data);
					statement.execute();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
      			}	 
      			myReader.close(); 
    		} 
		catch (FileNotFoundException e) { 
      			System.out.println("An error occurred."); 
      			e.printStackTrace(); 
    		} 
  	}
	*/

	/* 
	public void fillTables() 
	{ 
        	String[] files = {"MakeTables.sql", "Titles.sql", "Genres.sql", "People.sql", "Professions.sql",
						"AlternateTitles.sql", "TitleGenres.sql", "PeopleProfessions.sql", "KnownFor.sql",
						"HaveCrew.sql", "Movie.sql", "TVShow.sql", "Episode.sql"};
    		for (int i = 0; i < files.length; i++) 
		{ 
			try 
			{ 
				System.out.println(files[i]); 
				File myObj = new File("./SQL_Files/" + files[i]); 
				Scanner myReader = new Scanner(myObj); 
				while (myReader.hasNextLine()) 
				{ 
					String data = myReader.nextLine(); 
					try
					{
						PreparedStatement statement = connection.prepareStatement(data);
						statement.execute();
					} catch (SQLException e) {
						e.printStackTrace(System.out);
					}
				} 
				myReader.close(); 
			} catch (FileNotFoundException e) { 
				System.out.println("An error occurred."); 
				e.printStackTrace(); 
			} 
    		} 
  	}
	*/
	public String truncateString(String text, int length)
	{
		if(text.length() <= length) return text;
		else return text.substring(0,length);
	}
	
}
