
#### test file for sparkr. this code only handles a small state of AK.

library(SparkR)
# Sys.setenv("SPARKR_SUBMIT_ARGS"="--master yarn-client sparkr-shell")

rm(list=ls())
PkgTest <- function(x) {
  if (!require(x, character.only = TRUE)) {
    install.packages(x, dep = TRUE)
    if (!require(x, character.only = TRUE)) {
      stop("Package not found")
    }
  }
}

## These lines load the required packages
packages <- c("readxl", "data.table", "rgdal", "sp")
lapply(packages, PkgTest)

sparkR.session(ter = "local[*]", sparkConfig = list(spark.driver.memory = "4g"))
# setLogLevel("ERROR")

dir <- "/gpfs01/hadoop/user/cng10/Erick/zillow/stores/"
dir2 <- paste(dir, "02/", sep = "")

#("../stores/", 'Layout.xlsx'), sheet = 1)
layoutZAsmt <- read_excel(file.path(dir, 'Layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'Layout.xlsx'), sheet = 2, col_types = c("text", "text", "numeric", "text", "text"))

print("finished loading layout")

rows2load <- -1 # load everything
options(scipen = 999) # Do not print scientific notation
options(stringsAsFactors = FALSE) ## Do not load strings as factors

    ##  Create property attribute table
    #    Need 3 tables
    #    1) Main table in assessor database
    #    2) Building table
    #    3) BuildingAreas

col_namesMain <- t(layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName'])
col_namesBldg <- t(layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName'])
col_namesBldgA <- t(layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName'])

base <- read.table(file.path(dir2, "ZAsmt/Main.txt"),
                       nrows = rows2load,
                       sep = '|',
                       header = FALSE,
                       stringsAsFactors = FALSE,
                       skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                       comment.char="",                           # tells R not to read any symbol as a comment
                       quote = "",                                # this tells R not to read quotation marks as a special symbol
                       col.names = col_namesMain
)

print("finished reading table")
# object.size()
#### using sparkr dataframe object
base1 <- as.DataFrame(base)

print("finished converting dataframe")

## select the useful cols
base1 <- select(base1, list("RowID", "ImportParcelID", "LoadID", "FIPS", "State", "County",
                            "PropertyFullStreetAddress", "PropertyHouseNumber",
                            "PropertyHouseNumberExt", "PropertyStreetPreDirectional",
                            "PropertyStreetName", "PropertyStreetSuffix", "PropertyStreetPostDirectional",
                            "PropertyCity", "PropertyState", "PropertyZip",
                            "PropertyBuildingNumber", "PropertyAddressUnitDesignator", "PropertyAddressUnitNumber",
                            "PropertyAddressLatitude", "PropertyAddressLongitude", "PropertyAddressCensusTractAndBlock",
                            "NoOfBuildings", "LotSizeAcres", "LotSizeSquareFeet", "TaxAmount", "TaxYear"))

##### TODO: convert this part into SparkR style.
    if (length(unique(base$ImportParcelID)) != dim(base)[1] ) {
      #Example: Print all entries for parcels with at least two records.
      base[ImportParcelID %in% base[duplicated(ImportParcelID), ImportParcelID], ][order(ImportParcelID)]

      setkeyv(base, c("ImportParcelID", "LoadID"))  # Sets the index and also orders by ImportParcelID, then LoadID increasing
      keepRows <- base[ , .I[.N], by = c("ImportParcelID")]   # Creates a table where the 1st column is ImportParcelID and the second column
      # gives the row number of the last row that ImportParcelID appears.
      base <- base[keepRows[[2]], ] # Keeps only those rows identified in previous step
    }


print("hello, world.")
