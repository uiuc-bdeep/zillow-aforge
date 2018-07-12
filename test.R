
#### test file for sparkr. this code only handles a small state of AK.

#### this test temporarily partitions the top 100 rows of the dataset, for the
####  sake of the memory error..

library(SparkR)
## from https://stackoverflow.com/questions/38577939/not-able-to-retrieve-data-from-sparkr-created-dataframe
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

sparkR.session()
# namedConfig <- sparkR.conf("spark.executor.memory", "4g")
# setLogLevel("ERROR")

dir <- "/gpfs01/hadoop/user/cng10/Erick/zillow/stores/"
dir2 <- paste(dir, "46/", sep = "")

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







##################
####   BASE   ####
##################

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

# base_s <- head(base, n = 100L)

print("finished reading table")
print(object.size(base))
print(dim(base))
# object.size()
#### using sparkr dataframe object
base1 <- as.DataFrame(base, numPartitions = 100)
#base1 <- as.DataFrame(as.matrix(base))

#base1 <- read.text(file.path(dir2, "ZAsmt/Main.txt"))
#base1 <- read.df(paste0(dir2, "ZAsmt/Main.txt"), source = "csv", delimiter = "|", header="true", inferSchema="true")

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

print("finished selection")
##### TODO: convert this part into SparkR style.
    if (length(unique(base$ImportParcelID)) != dim(base)[1] ) {
      #Example: Print all entries for parcels with at least two records.
      base[ImportParcelID %in% base[duplicated(ImportParcelID), ImportParcelID], ][order(ImportParcelID)]

      setkeyv(base, c("ImportParcelID", "LoadID"))  # Sets the index and also orders by ImportParcelID, then LoadID increasing
      keepRows <- base[ , .I[.N], by = c("ImportParcelID")]   # Creates a table where the 1st column is ImportParcelID and the second column
      # gives the row number of the last row that ImportParcelID appears.
      base <- base[keepRows[[2]], ] # Keeps only those rows identified in previous step
    }

print(base1)








##################
####   BLDG   ####
##################



######################################################################
#### Load most property attributes

bldg <- read.table(file.path(dir2, "ZAsmt/Building.txt"),
                   nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                   sep = '|',
                   header = FALSE,
                   stringsAsFactors = FALSE,
                   skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                   comment.char="",                           # tells R not to read any symbol as a comment
                   quote = "",                                # this tells R not to read quotation marks as a special symbol
                   col.names = col_namesBldg
)

print("finished read.table")

# bldg_s <- head(bldg, n = 100L)

bldg <- as.DataFrame(bldg, numPartition = 100)

print("finished as.DataFrame")
bldg <- select(bldg, c("RowID", "NoOfUnits", "BuildingOrImprovementNumber",
                     "YearBuilt", "EffectiveYearBuilt", "YearRemodeled",
                     "NoOfStories", "StoryTypeStndCode", "TotalRooms", "TotalBedrooms",
                     "FullBath", "ThreeQuarterBath", "HalfBath", "QuarterBath",
                     "HeatingTypeorSystemStndCode",
                     "PropertyLandUseStndCode", "WaterStndCode"))

print("finished select")
#  Reduce bldg dataset to Single-Family Residence, Condo's, Co-opts (or similar)

bldg <- filter(bldg, bldg$PropertyLandUseStndCode %in% c('RR101',  # SFR
                                                         'RR999',  # Inferred SFR
                                                         'RR104',  # Townhouse
                                                         'RR105',  # Cluster Home
                                                         'RR106',  # Condominium
                                                         'RR107',  # Cooperative
                                                         'RR108',  # Row House
                                                         'RR109',  # Planned Unit Development
                                                         'RR113',  # Bungalow
                                                         'RR116',  # Patio Home
                                                         'RR119',  # Garden Home
                                                         'RR120')) # Landominium

print("finished reducing bldg dataset")






###### merging !!!!!
attr <- merge(base1, bldg, by = "RowID")

print("finished merging")
print(attr)



print("hello, world.")
