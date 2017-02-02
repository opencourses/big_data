# Exercise 46

## Description

PoliBooks is a company selling books. The management of PoliBooks is interested in analyzing their customers and books.
The analyses are based on the following data sets/files.

 - BoughtBooks.txt
	o BoughtBooks.txt is a textual file containing the list of books that have been
	bought (purchased) by the customers of PoliBooks
	o Every time a customer buys a book a new line is appended at the end of
	BoughtBooks.txt, i.e., each line of the file contains the information about one
	purchase
	o Each line of the file has the following format
		- customerid,bookid,timestamp,price
	Where customerid is a customer identifier, bookid is the identifier of a book, timestamp is the time at which customerid bought/purchased bookid and price is the cost of the purchase.
		- For example, the line customer1,book122,20160506_23:10,14.99
		means that customer1 bought book122 on May 6, 2016 at 23:10 and the price of the purchase was 14.99 euro

 - Books.txt
	o Books.txt is a textual file containing the list of available books with the
	associated characteristics
	o The file contains one single line for each book o Each line of the file has the following format
	- bookid,title,author,suggested_price
	Where, bookid is the identifier of a book, title is its title, author is the author of the book, and suggested_price is the price suggested by the editor of the book.
	- For example, the line
	book122,The Body in the Library,Agatha Christie,25.19
	means that the title of book122 is “The Body in the Library”, the author is “Agatha Christie”, and the suggested price is 25.19 euro.
