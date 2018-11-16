use csvcatalog;

select * from catalog_tables;

# tools example
delete from catalog_tables where table_name="people";
insert into catalog_columns values ("shit", "shit", "yes", "text");
update catalog_columns set is_nullable="no" where table_name="shit";

select * from catalog_columns;