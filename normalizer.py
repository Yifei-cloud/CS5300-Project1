from schema_parser import SchemaParser
from numpy import isin


class Normalizer:
    
    def __init__(self, schema_parser):
        self.schema_parser = schema_parser
        self.tables_1nf = {}
        self.tables_2nf = {}
        self.tables_3nf = {}
        self.tables_bcnf = {}
        self.tables_4nf = {}
        self.normalize_to_1nf()  # Automatically normalize to 1NF on initialization


    def normalize_to_1nf(self):
        
        base_table = self.schema_parser.tables["CoffeeShopData"].copy()
        fd_non_atomic = self.schema_parser.fd_non_atomic


        for index, (left, right) in enumerate(fd_non_atomic):
            right_cleaned = [attr.replace("(a non-atomic attribute)", "").strip() for attr in right]
            new_table_name = f"Table_{index + 1}_1NF  (Already in 5NF)"
            new_table_attributes = left + right_cleaned
            self.tables_1nf[new_table_name] = {
                "attributes": new_table_attributes,
                "primary_key": left,
                "fd": f"{left} --> {right_cleaned}"
            }
            # Remove the non-atomic attributes from the original base table
            base_table = [attr for attr in base_table if attr not in right_cleaned]


        # Store the modified base table as Table_Original_1NF
        if "Table_Original_1NF" not in self.tables_1nf:
            self.tables_1nf["Table_Original_1NF"] = {
                "attributes": base_table,
                "primary_key": self.schema_parser.primary_keys,
                "fd": "Any FDs but the non-atomic ones"
            }
 
        print("\n1NF Tables:")
        for table_name, details in self.tables_1nf.items():
            print(f"{table_name}:\n  Attributes: {details['attributes']}\n  Primary Key: {details['primary_key']}\n  Functional Dependency: {details['fd']}\n")

    def normalize_to_2nf(self):
        
        # Normalize the Table_Original_1NF to 2NF by removing partial dependencies.
        base_table_info = self.tables_1nf["Table_Original_1NF"]
        primary_key = base_table_info["primary_key"]
        remaining_fds = self.schema_parser.functional_dependencies


        new_base_attributes = base_table_info["attributes"].copy()


        for index, (left, right) in enumerate(remaining_fds):
            # Check if left-hand side is a subset of the primary key (partial dependency)
            if set(left).issubset(set(primary_key)) and set(left) != set(primary_key):
                # Create a new table for this partial dependency
                new_table_name = f"Table_Partial_{index + 1}_2NF"
                new_table_attributes = left + right
                self.tables_2nf[new_table_name] = {
                    "attributes": new_table_attributes,
                    "primary_key": left,
                    "fd": f"{left} --> {right}"
                }


                # Remove right-hand side attributes from the original base table if not part of the primary key
                for attr in right:
                    if attr not in primary_key:
                        new_base_attributes = [a for a in new_base_attributes if a != attr]


        # Update the modified base table as Table_Original_2NF.
        # Also, set does not allow duplcate item, I believe the 4th one is the MVD table.
        # Hence, the newest 2NF-origirnal table replaced the MVD table, which is very good.
        self.tables_2nf["Table_Original_2NF"] = {
            "attributes": new_base_attributes,
            "primary_key": primary_key,
            "fd": "removes partial dependencies, but still include all FDs(MVD one as well),"
        }


        print("\n2NF Tables:")
        for table_name, details in self.tables_2nf.items():
            print(f"{table_name}:\n  Attributes: {details['attributes']}\n  Primary Key: {details['primary_key']}\n  Functional Dependency: {details['fd']}\n")
    
    def normalize_to_3nf(self):
        # Iterate through each table created in 2NF
        # Detect transitive dependencies
        remaining_fds = self.schema_parser.functional_dependencies
        decomposed = True  # Flag to control the loop until no more decompositions are needed

        while decomposed:
            decomposed = False
            for i, (left_fd, right_fd) in enumerate(remaining_fds):
                for j, (left_other, right_other) in enumerate(remaining_fds):
                    if i != j:  # Ensure we're not comparing the same FD
                        # Check for transitive dependency: left side in right side of another FD
                        transitive_attr = set(left_fd).intersection(set(right_other))
                        if transitive_attr:
                            transitive_attr = list(transitive_attr)[0]  # Get the matched attribute

                            # Decompose the related tables
                            decomposed = True
                            new_table_name = f"Table_3NF_{transitive_attr}_from_FD_{i+1}"
                            self.tables_3nf[new_table_name] = {
                                "attributes": left_fd + right_fd,
                                "primary_key": left_fd,
                                "fd": f"{left_fd} --> {right_fd}"
                            }
                            print(f"Created table {new_table_name} with attributes {self.tables_3nf[new_table_name]['attributes']}")

                            # Update the original table
                            updated_attributes = [attr for attr in left_other + right_other if attr != transitive_attr]
                            original_table_name = f"Table_Original_3NF_{j+1}"
                            self.tables_3nf[original_table_name] = {
                                "attributes": updated_attributes,
                                "primary_key": left_other,
                                "fd": f"{left_other} --> {updated_attributes}"
                            }
                            print(f"Updated table {original_table_name} with attributes {updated_attributes}")

                            # Remove the transitive dependency attribute from the right side of original table
                            remaining_fds[j] = (left_other, [attr for attr in right_other if attr != transitive_attr])
                
                # Print tables that passed 3NF, but I don't think needed
                for table_name, details in self.tables_2nf.items():
                    if table_name not in self.tables_3nf:
                        self.tables_3nf[table_name] = {
                        "attributes": details["attributes"],
                        "primary_key": details["primary_key"],
                        "fd": "same as in 2NF and Passes 3NF"
                        }

        print("\n3NF Tables:")
        for table_name, details in self.tables_3nf.items():
            print(f"{table_name}:\n  Attributes: {details['attributes']}\n  Primary Key: {details['primary_key']}\n  Functional Dependency: {details['fd']}\n")

    
    def normalize_to_bcnf(self):
         
        for table_name, details in self.tables_3nf.items():
            # Check if the left side of FD is the primary key for each table
            if details["fd"].split(" --> ")[0].strip() == str(details["primary_key"]):
                self.tables_bcnf[table_name] = {
                    "attributes": details["attributes"],
                    "primary_key": details["primary_key"],
                    "fd": "Passes BCNF"
                }
            else:
                # Table passes BCNF, just mark it as such
                self.tables_bcnf[table_name] = details

        print("\nBCNF Tables: printed 2nf tables also past BCNF\n")
        for table_name, details in self.tables_bcnf.items():
            print(f"{table_name}:\n  Attributes: {details['attributes']}\n  Primary Key: {details['primary_key']}\n  Functional Dependency: {details['fd']}\n")
    
    def normalize_to_4nf(self):
        
        mvd_fds = self.schema_parser.multivalued_dependencies

        if not mvd_fds:
            print("\nNo MVDs found, all tables pass 4NF.")
            return

        for index, (left, right) in enumerate(mvd_fds):
            # Clean the right attributes of "(a MVD)" phrase with '|'
            right_cleaned = [
                sub_attr.strip() 
                for attr in right 
                for sub_attr in attr.replace("(a MVD)", "").strip().split("|")
            ]
            print(f"\nProcessing MVD {left} --> {right_cleaned}")

            # Locate a matching table in 2NF by checking if all MVD attributes are present in any table(in our case only 1)
            found_table = False
            for table_name, table_info in self.tables_2nf.items():           
                if set(left + right_cleaned).issubset(set(table_info["attributes"])):
                    found_table = True

                    # Remove the original table, as it's decomposed
                    del self.tables_2nf[table_name]

                    # Decompose this table based on the MVD, creating a new table for each attribute in the right side
                    for single_attr in right_cleaned:
                        mvd_table_name = f"Table_4NF_MVD_{index + 1}_{single_attr}"
                        mvd_table_attributes = left + [single_attr]
                        self.tables_4nf[mvd_table_name] = {
                            "attributes": mvd_table_attributes,
                            "primary_key": left,
                            "fd": f"{left} --> {single_attr}"
                        }
                        print(f"Created table {mvd_table_name} with attributes {mvd_table_attributes}")

                    # Do not create a "remaining attributes" table
                    print(f"Decomposed {table_name} into individual MVD tables.")
                    break
            
            if not found_table:
                print(f"Debug: No matching table found in 2NF for MVD {left} --> {right_cleaned}")

        print("\n4NF Tables:\n")
        for table_name, details in self.tables_4nf.items():
            print(f"{table_name}:\n  Attributes: {details['attributes']}\n  Primary Key: {details['primary_key']}\n  Functional Dependency: {details['fd']}\n")

    def selection(self):
            while True:
                choice = input("Choose normalization level (1 for 1NF, 2 for 2NF, 3 for 3NF, BCNF, 4 for 4NF, q to quit): ").strip()
                if choice == '1':
                    self.normalize_to_1nf()
                elif choice == '2':
                    self.normalize_to_1nf()
                    self.normalize_to_2nf()
                elif choice == '3':
                    self.normalize_to_1nf()
                    self.normalize_to_2nf()
                    self.normalize_to_3nf()
                elif choice.upper() == 'BCNF':
                    self.normalize_to_1nf()
                    self.normalize_to_2nf()
                    self.normalize_to_3nf()
                    self.normalize_to_bcnf()
                elif choice == '4':
                    self.normalize_to_1nf()
                    self.normalize_to_2nf()
                    self.normalize_to_3nf()
                    self.normalize_to_bcnf()
                    self.normalize_to_4nf()
                elif choice.lower() == 'q':
                    print("Exiting.")
                    break
                else:
                    print("Invalid choice. Please enter 1, 2, 3, BCNF, 4, or q.")

# Usage without modifying the main file
if __name__ == "__main__":
    file_path = "Sample Data.xlsx"
    parser = SchemaParser(file_path)
    normalizer = Normalizer(parser)
    normalizer.selection()