# Insignia Data Warehouse Project

This repository contains the complete implementation of the Insignia Corporation's Data Warehouse solution, including the data model design, ETL scripts, dimension handling logic (SCD Types 1, 2, and 3), and lineage tracking for data governance.

## 📁 Project Structure

```
/InsigniaDW/
│
├── SQL_Scripts/
│   ├── Create_Tables.sql
│   ├── Load_Dimensions.sql
│   ├── Load_FactSales.sql
│   ├── Load_Lineage.sql
│   └── Date_Dimension_Generator.sql
│
├── Documentation/
│   ├── Insignia_ETL_Documentation.docx
│   └── Insignia_Data_Model_Diagram_Improved.png
│
├── Staging/
│   ├── Insignia_staging.xlsx
│   └── Insignia_incremental.xlsx
│
└── README.md
```

## 🚀 How to Run

### 1. Create and switch to the database
```sql
CREATE DATABASE InsigniaDW;
USE InsigniaDW;
```

### 2. Create Tables
Execute `Create_Tables.sql` to generate all dimension, fact, and lineage tables.

### 3. Generate Date Dimension
Use the script `Date_Dimension_Generator.sql` to populate the Date Dimension from the year 2000 to 2023.

### 4. Load Initial Staging Copy
Truncate and insert data from `Insignia_incremental` into `Insignia_staging_copy`.

### 5. Load Dimensions
Run `Load_Dimensions.sql` to populate all dimension tables:
- `DimCustomer` (SCD Type 2)
- `DimEmployee` (SCD Type 2)
- `DimGeography` (SCD Type 3)
- `DimProduct` (SCD Type 1)

### 6. Load FactSales
Execute `Load_FactSales.sql` to insert into the central fact table.

### 7. Insert Lineage Info
Use `Load_Lineage.sql` to track ETL metadata such as load start/end time, source count, destination count, and status.

## 🧠 Features Implemented

- Full dimensional modeling (Star Schema)
- SCD Type 1, 2, 3 implementation using `LEFT JOIN` + `UPDATE`
- Late arriving dimension handling
- ETL Lineage logging
- Optimized query performance with indexes
- Data model diagram included

## 📝 Documentation

All steps and screenshots are documented in:  
📄 `Documentation/Insignia_ETL_Documentation.docx`



## 👨‍💻 Author
Sucharita Gorai


---

> Note: No MERGE statements were used. All SCD logic follows best practice using `LEFT JOIN` + `UPDATE`.
