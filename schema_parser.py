import pandas as pd

class SchemaParser:
    def __init__(self, file_path):
        """
        Initialize the SchemaParser class and load the Excel file.

        Parameters:
            file_path (str): Path to the Excel file.
        """
        self.file_path = file_path
        self.tables = {"CoffeeShopData": []}
        self.primary_keys = []
        self.functional_dependencies = []
        self.fd_non_atomic = []
        self.multivalued_dependencies = []
        self.load_schema()

    def load_schema(self):
        # load schema details by identifying primary keyS, functional dependencies, and multivalued dependencies.
        try:
            sheet = pd.read_excel(self.file_path, sheet_name='Sheet1')
            
            # Extract column names as fields for 'CoffeeShopData' table
            fields = sheet.iloc[0, :].tolist()
            self.tables["CoffeeShopData"] = [field for field in fields if pd.notna(field)]

            # Scan for primary keys and dependencies
            for idx, row in sheet.iterrows():
                for cell in row.dropna():
                    cell_text = str(cell)
                    # Identify primary key
                    if "Primary Key:" in cell_text:
                        self.primary_keys = self.extract_primary_key(cell_text)
                    # Identify functional or multivalued dependency
                    elif "-->" in cell_text or "-->>" in cell_text:
                        self.classify_dependency(cell_text)

        except Exception as e:
            print("Error loading file:", e)

    def extract_primary_key(self, primary_key_text):
        primary_key_text = primary_key_text.replace("Primary Key:", "").strip()
        primary_keys = [key.strip() for key in primary_key_text.strip("{} ").split(",")]
        return primary_keys

    def classify_dependency(self, dependency_text):
        if "-->>" in dependency_text:  # Check for MVD
            left, right = dependency_text.split("-->>")
            left_attributes = [attr.strip() for attr in left.strip("{} ").split(",")]
            right_attributes = [attr.strip() for attr in right.strip("{} ").split(",")]
            #if "MVD" in dependency_text:
            self.multivalued_dependencies.append((left_attributes, right_attributes))
            #else:
            #     If MVD keyword is missing, treat as a standard FD for flexibility
            #    self.functional_dependencies.append((left_attributes, right_attributes))
        elif "-->" in dependency_text:  # Check for regular FD and FD-NonAtomic
            left, right = dependency_text.split("-->")
            left_attributes = [attr.strip() for attr in left.strip("{} ").split(",")]
            right_attributes = [attr.strip() for attr in right.strip("{} ").split(",")]
            if "non-atomic" in dependency_text:
                self.fd_non_atomic.append((left_attributes, right_attributes))
            else:
                self.functional_dependencies.append((left_attributes, right_attributes))

# Testing code
if __name__ == "__main__":
    file_path = "Sample Data.xlsx"
    parser = SchemaParser(file_path)
    print("Table Fields:", parser.tables)
    print("Primary Keys:", parser.primary_keys)
    print("Functional Dependencies:", parser.functional_dependencies)
    print("FD-NonAtomic Dependencies:", parser.fd_non_atomic)
    print("Multivalued Dependencies:", parser.multivalued_dependencies)






