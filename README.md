# Final_Project

This is the Final Project for Group 9 in COMP3380,W2025.

You can build the program using 'make' and run the program with 'make run'

On start, the program will attempt to connect to the University of Manitoba's Uranium Server. 
There should be a file named 'auth.cfg' that came with this program. Do not delete this file,
as the program will use it to login.
The program will then automatically delete and re-fill the database. This can be triggered
manually as well (see section 'Maintenance Mode').

# Basic Use

The program is split into different Modes. You can only run a selection of commands in one mode.
Any attempt to use a command outside of its intended mode will fail and remind the user how to access the help menu.
The program starts in Welcome Mode, a Mode that only serves to choose which Mode the user wishes to start in.

# Help Menu and Commands

While in a Mode, you can type 'help' to see the available commands in the current Mode.
Commands follow a general format of prefix, secondary prefix, <\argument one> <\argument two>.
Dates always follow the format of YYYYMMDD (no spaces or dashes).

The help menu will also show you which numbers correspond to which Modes:
1 for Browse Mode, 2 for Search Mode, 3 for Maintenance Mode.

# Browse Mode

Browse Mode hosts commands that perform basic lookup queries that aid in advanced queries in Search Mode,
 and provide some previews of data:

a w: Prints all Wards in the database

a c: Prints all Councillors in the database

a n: Prints all Neighbourhoods in the database

a t: Prints all Third-Parties in the database (!This may return a large amount of text. Consider using b n or b own instead.)

a b: Prints all registered business owners in the database

w <\name>: Search for a Ward using name

c n <\name>: Search for a Councillor using name

c h <\neighbourhoodName>: Seach for a Councillor representing a neighbourhood, searching by neighbourhoodName

c y <\YYYY>: Search for a Councillor by the year they served, in YYYY format

b n <\name>: Search for a business by name
b own <\ownerName>: Search for a business by the owner's name (if in database)

# Search Mode

Search Mode hosts commands that perform more interesting lookups of relationships between the entities in the database:

c e <\CID>: Get a list of a Councillor's expenses by CID (you can find a Councillor's CID in Browse Mode by using any Councillor Search command)

c g <\CID>: Get a list of a Councillor's gifts received by CID

c l <\CID>: Get a list of a Councillor's lobby activities participated in by CID

r e <\YYYYMMDD>: Show all expenses made in a date range

r g <\YYYYMMDD>: Show all gifts received in a date range

n <\YYYYMMDD> <\YYYYMMDD>: Show Councillors that have not participated in any lobby activities or received any gifts in a date range (remember the lobby caveat!)

v e <\CID>: Get expendature total by CID

v g <\CID>: Get total known value of gifts received by CID

t e: Show top 10 Councillors that spend the most (excluding on gifts)

s: Show top 10 gifts that were given closest to an election

g: Show top 10 gifts with the largest gap between date recorded and date received

l: Show top 10 third-parties that have given gifts and/or performed lobby activities

# Maintenance Mode

This Mode only hosts commands to delete and re-populate the database.
Any attempt to switch to another mode while no data is present will fail.

deleteTables: Deletes all data from the database

populateTables: Re-populates all data into the database




