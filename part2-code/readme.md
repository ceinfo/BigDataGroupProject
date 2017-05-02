
# Final Project - Part 2

This portion of the project combines the NY Open Data Crime Dataset with the American Community Survey (ACS) demographic dataset.  Specifically,  we used the PUMA generated Neighborhood Tabulaton Area (NTA) data.  The American Community Survey (ACS) which is program run by the U.S. Cenus Bureau that collects continuing statistical data about American's population, housing, and workforce data.  In our project, we focus on the ACS data for the New York area.


## 1- Generate the Demographic Data
In this section, we normalize the ACS dataset and generate the new output into "ce_{filname}.csv" files.  These new output files are used to find interesting elements of the NY Crime Dataset.


**1) General Informaton:**

    1) Location:  part2-code/create_population_data
    2) The Demographic_intermediary_csv.zip file is originally generated from the Demographics_20and_20profiles_20at_20the_20Neighborhood_20Tabulation_20Area_20_NTA__20level.zip file. 
        * ACS Dataset Link:  https://data.cityofnewyork.us/City-Government/Demographic-Social-Economic-and-Housing-Profiles-b/kvuc-fg9b
        * Save each xls file as a "Windows Comman Separated (.csv)"
     

**2) Run Command:** 

```
cd create_demographic_data
unzip Demographic_intermediary_csv.zip
```

**3) Dataset Modifies To:**

```
Original Dataset: 
,BK72 Williamsburg,,,,BK73 North Side-South Side,,,,BK76 Greenpoint,,,,BK90 East Williamsburg,,,,BK09 Brooklyn Heights-Cobble Hill,BK09 Brooklyn Heights-Cobble Hill,BK09 Brooklyn Heights-Cobble Hill,BK09 Brooklyn Heights-Cobble Hill,BK38 DUMBO-Vinegar Hill-Downtown Brooklyn-Boerum Hill,BK38 DUMBO-Vinegar Hill-Downtown Brooklyn-Boerum Hill,BK38 DUMBO-Vinegar Hill-Downtown Brooklyn-Boerum Hill,BK38 DUMBO-Vinegar Hill-Downtown Brooklyn-Boerum Hill,BK68 Fort Greene,,,,BK69 Clinton Hill,,,,BK35 Stuyvesant Heights,BK35 Stuyvesant Heights,BK35 Stuyvesant Heights,BK35 Stuyvesant Heights,BK75 Bedford,,,,BK77 Bushwick North,,,,BK78 Bushwick South,,,,BK99 park-cemetery-etc-Brooklyn,,,,BK82 East New York,,,,BK83 Cypress Hills-City Line,,,,BK93 Starrett City,,,,BK33 Carroll Gardens-Columbia Street-Red Hook,BK33 Carroll Gardens-Columbia Street-Red Hook,BK33 Carroll Gardens-Columbia Street-Red Hook,BK33 Carroll Gardens-Columbia Street-Red Hook,BK37 Park Slope-Gowanus,BK37 Park Slope-Gowanus,BK37 Park Slope-Gowanus,BK37 Park Slope-Gowanus,BK32 Sunset Park West,BK32 Sunset Park West,BK32 Sunset Park West,BK32 Sunset Park West,BK34 Sunset Park East,BK34 Sunset Park East,BK34 Sunset Park East,BK34 Sunset Park East,BK40 Windsor Terrace,BK40 Windsor Terrace,BK40 Windsor Terrace,BK40 Windsor Terrace,BK61 Crown Heights North,,,,BK64 Prospect Heights,,,,BK60 Prospect Lefferts Gardens-Wingate,,,,BK63 Crown Heights South,,,,BK30 Dyker Heights,BK30 Dyker Heights,BK30 Dyker Heights,BK30 Dyker Heights,BK31 Bay Ridge,BK31 Bay Ridge,BK31 Bay Ridge,BK31 Bay Ridge,BK27 Bath Beach,BK27 Bath Beach,BK27 Bath Beach,BK27 Bath Beach,BK28 Bensonhurst West,BK28 Bensonhurst West,BK28 Bensonhurst West,BK28 Bensonhurst West,BK29 Bensonhurst East,BK29 Bensonhurst East,BK29 Bensonhurst East,BK29 Bensonhurst East,BK41 Kensington-Ocean Parkway,,,,BK46 Ocean Parkway South,,,,BK88 Borough Park,,,,BK19 Brighton Beach,BK19 Brighton Beach,BK19 Brighton Beach,BK19 Brighton Beach,BK21 Seagate-Coney Island,BK21 Seagate-Coney Island,BK21 Seagate-Coney Island,BK21 Seagate-Coney Island,BK23 West Brighton,BK23 West Brighton,BK23 West Brighton,BK23 West Brighton,BK26 Gravesend,BK26 Gravesend,BK26 Gravesend,BK26 Gravesend,BK42 Flatbush,,,,BK43 Midwood,,,,BK17 Sheepshead Bay-Gerritsen Beach-Manhattan Beach,BK17 Sheepshead Bay-Gerritsen Beach-Manhattan Beach,BK17 Sheepshead Bay-Gerritsen Beach-Manhattan Beach,BK17 Sheepshead Bay-Gerritsen Beach-Manhattan Beach,BK25 Homecrest,BK25 Homecrest,BK25 Homecrest,BK25 Homecrest,BK44 Madison,,,,BK79 Ocean Hill,,,,BK81 Brownsville,,,,BK85 East New York (Pennsylvania Ave),,,,BK91 East Flatbush-Farragut,,,,BK95 Erasmus,,,,BK96 Rugby-Remsen Village,,,,BK45 Georgetown-Marine Park-Bergen Beach-Mill Basin,,,,BK50 Canarsie,,,,BK58 Flatlands,,,,BX27 Hunts Point,,,,BX33 Longwood,,,,BX34 Melrose South-Mott Haven North,,,,BX39 Mott Haven-Port Morris,,,,BX98 Rikers Island1,,,,BX01 Claremont-Bathgate,,,,BX06 Belmont,,,,BX17 East Tremont,,,,BX35 Morrisania-Melrose,,,,BX75 Crotona Park East,,,,BX14 East Concourse-Concourse Village,,,,BX26 Highbridge,,,,BX63 West Concourse,,,,BX36 University Heights-Morris Heights,,,,BX40 Fordham South,,,,BX41 Mount Hope,,,,BX05 Bedford Park-Fordham North,,,,BX30 Kingsbridge Heights,,,,BX43 Norwood,,,,BX22 North Riverdale-Fieldston-Riverdale,,,,BX28 Van Cortlandt Village,,,,BX29 Spuyten Duyvil-Kingsbridge,,,,BX08 West Farms-Bronx River,,,,BX09 Soundview-Castle Hill-Clason Point-Harding Park,,,,BX46 Parkchester,,,,BX55 Soundview-Bruckner,,,,BX59 Westchester-Unionport,,,,BX10 Pelham Bay-Country Club-City Island,,,,BX13 Co-op City,,,,BX52 Schuylerville-Throgs Neck-Edgewater Park,,,,BX07 Bronxdale,,,,BX31 Allerton-Pelham Gardens,,,,BX37 Van Nest-Morris Park-Westchester Square,,,,BX49 Pelham Parkway,,,,BX03 Eastchester-Edenwald-Baychester,,,,BX44 Williamsbridge-Olinville,,,,BX62 Woodlawn-Wakefield,,,,BX99 park-cemetery-etc-Bronx,,,,MN23 West Village,,,,MN24 SoHo-TriBeCa-Civic Center-Little Italy,,,,MN25 Battery Park City-Lower Manhattan,,,,MN22 East Village,,,,MN27 Chinatown,,,,MN28 Lower East Side,,,,MN13 Hudson Yards-Chelsea-Flat Iron-Union Square,,,,MN15 Clinton,,,,MN17 Midtown-Midtown South,,,,MN19 Turtle Bay-East Midtown,,,,MN20 Murray Hill-Kips Bay,,,,MN21 Gramercy,,,,MN50 Stuyvesant Town-Cooper Village,,,,MN12 Upper West Side,,,,MN14 Lincoln Square,,,,MN31 Lenox Hill-Roosevelt Island,,,,MN32 Yorkville,,,,MN40 Upper East Side-Carnegie Hill,,,,MN04 Hamilton Heights,,,,MN06 Manhattanville,,,,MN09 Morningside Heights,,,,MN03 Central Harlem North-Polo Grounds,,,,MN11 Central Harlem South,,,,MN33 East Harlem South,,,,MN34 East Harlem North,,,,MN01 Marble Hill2-Inwood,,,,MN35 Washington Heights North,,,,MN36 Washington Heights South,,,,MN99 park-cemetery-etc-Manhattan,,,,QN68 Queensbridge-Ravenswood-Long Island City,,,,QN70 Astoria,,,,QN71 Old Astoria,,,,QN72 Steinway,,,,QN99 park-cemetery-etc-Queens,,,,QN31 Hunters Point-Sunnyside-West Maspeth,,,,QN50 Elmhurst-Maspeth,,,,QN63 Woodside,,,,QN26 North Corona,,,,QN27 East Elmhurst,,,,QN28 Jackson Heights,,,,QN98 Airport,,,,QN25 Corona,,,,QN29 Elmhurst,,,,QN19 Glendale,,,,QN20 Ridgewood,,,,QN21 Middle Village,,,,QN30 Maspeth,,,,QN17 Forest Hills,,,,QN18 Rego Park,,,,QN22 Flushing,,,,QN23 College Point,,,,QN47 Ft. Totten-Bay Terrace-Clearview,,,,QN49 Whitestone,,,,QN51 Murray Hill,,,,QN52 East Flushing,,,,QN62 Queensboro Hill,,,,QN06 Jamaica Estates-Holliswood,,,,QN35 Briarwood-Jamaica Hills,,,,QN37 Kew Gardens Hills,,,,QN38 Pomonok-Flushing Heights-Hillcrest,,,,QN41 Fresh Meadows-Utopia,,,,QN53 Woodhaven,,,,QN54 Richmond Hill,,,,QN60 Kew Gardens,,,,QN55 South Ozone Park,,,,QN56 Ozone Park,,,,QN57 Lindenwood-Howard Beach,,,,QN42 Oakland Gardens,,,,QN45 Douglas Manor-Douglaston-Little Neck,,,,QN46 Bayside-Bayside Hills,,,,QN48 Auburndale,,,,QN01 South Jamaica,,,,QN02 Springfield Gardens North,,,,QN07 Hollis,,,,QN08 St. Albans,,,,QN61 Jamaica,,,,QN76 Baisley Park,,,,QN03 Springfield Gardens South-Brookville,,,,QN05 Rosedale,,,,QN33 Cambria Heights,,,,QN34 Queens Village,,,,QN43 Bellerose,,,,QN44 Glen Oaks-Floral Park-New Hyde Park,,,,QN66 Laurelton,,,,QN10 Breezy Point-Belle Harbor-Rockaway Park-Broad Channel,,,,QN12 Hammels-Arverne-Edgemere,,,,QN15 Far Rockaway-Bayswater,,,,SI07 Westerleigh,,,,SI08 Grymes Hill-Clifton-Fox Hills,,,,SI12 Mariner's Harbor-Arlington-Port Ivory-Graniteville,,,,SI22 West New Brighton-New Brighton-St. George,,,,SI28 Port Richmond,,,,SI35 New Brighton-Silver Lake,,,,SI37 Stapleton-Rosebank,,,,SI05 New Springville-Bloomfield-Travis,,,,SI14 Grasmere-Arrochar-Ft. Wadsworth,,,,SI24 Todt Hill-Emerson Hill-Heartland Village-Lighthouse Hill,,,,SI36 Old Town-Dongan Hills-South Beach,,,,SI45 New Dorp-Midland Beach,,,,SI01 Annadale-Huguenot-Prince's Bay-Eltingville,,,,SI11 Charleston-Richmond Valley-Tottenville,,,,SI25 Oakwood-Oakwood Beach,,,,SI32 Rossville-Woodrow,,,,SI48 Arden Heights,,,,SI54 Great Kills,,,,SI99 park-cemetery-etc-Staten Island,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
Total households,"8,299",215,100.0, - ,"19,172",372,100.0, - ,"15,124",380,100.0, - ,"14,888",297,100.0, - ,"11,346",374,100.0, - ,"15,085",316,100.0, - ,"11,560",313,100.0, - ,"14,937",368,100.0, - ,"24,360",452,100.0, - ,"24,225",451,100.0, - ,"18,885",343,100.0, - ,"24,164",448,100.0, - ,170,80,100.0, - ,"30,771",490,100.0, - ,"13,634",342,100.0, - ,"6,378",158,100.0, - ,"17,628",387,100.0, - ,"31,068",489,100.0, - ,"17,023",318,100.0, - ,"20,544",326,100.0, - ,"9,510",205,100.0, - ,"40,818",573,100.0, - ,"9,394",270,100.0, - ,"26,016",421,100.0, - ,"14,662",272,100.0, - ,"14,966",279,100.0, - ,"34,990",426,100.0, - ,"10,726",270,100.0, - ,"29,717",419,100.0, - ,"21,737",342,100.0, - ,"12,037",287,100.0, - ,"6,607",235,100.0, - ,"28,325",452,100.0, - ,"13,764",343,100.0, - ,"11,215",272,100.0, - ,"8,026",219,100.0, - ,"11,068",297,100.0, - ,"37,482",454,100.0, - ,"18,430",320,100.0, - ,"25,070",401,100.0, - ,"15,435",318,100.0, - ,"15,022",240,100.0, - ,"11,686",319,100.0, - ,"20,316",363,100.0, - ,"9,841",248,100.0, - ,"18,217",316,100.0, - ,"10,222",280,100.0, - ,"20,055",391,100.0, - ,"16,432",289,100.0, - ,"27,669",396,100.0, - ,"23,279",315,100.0, - ,"8,877",186,100.0, - ,"8,318",227,100.0, - ,"12,776",282,100.0, - ,"17,497",288,100.0, - ,0,-,100.0, - ,"9,847",204,100.0, - ,"8,319",249,100.0, - ,"14,245",288,100.0, - ,"12,475",260,100.0, - ,"6,912",186,100.0, - ,"21,343",360,100.0, - ,"12,711",253,100.0, - ,"12,795",234,100.0, - ,"17,631",351,100.0, - ,"8,338",227,100.0, - ,"16,528",281,100.0, - ,"18,253",328,100.0, - ,"10,599",266,100.0, - ,"14,192",278,100.0, - ,"10,909",339,100.0, - ,"17,347",329,100.0, - ,"12,741",319,100.0, - ,"11,448",307,100.0, - ,"18,657",296,100.0, - ,"12,590",286,100.0, - ,"11,247",273,100.0, - ,"8,788",254,100.0, - ,"11,247",321,100.0, - ,"18,258",352,100.0, - ,"16,655",351,100.0, - ,"13,479",269,100.0, - ,"9,707",198,100.0, - ,"9,657",230,100.0, - ,"11,364",251,100.0, - ,"11,599",292,100.0, - ,"20,322",356,100.0, - ,"15,128",300,100.0, - ,482,53,100.0, - ,"37,218",714,100.0, - ,"18,761",519,100.0, - ,"18,931",563,100.0, - ,"22,306",477,100.0, - ,"18,107",418,100.0, - ,"30,216",429,100.0, - ,"39,072",730,100.0, - ,"25,261",639,100.0, - ,"15,964",499,100.0, - ,"29,094",754,100.0, - ,"26,173",724,100.0, - ,"14,877",492,100.0, - ,"10,481",243,100.0, - ,"66,941",959,100.0, - ,"32,169",851,100.0, - ,"43,515","1,005",100.0, - ,"43,613",800,100.0, - ,"28,751",702,100.0, - ,"18,678",409,100.0, - ,"8,130",224,100.0, - ,"20,166",423,100.0, - ,"33,203",589,100.0, - ,"18,506",412,100.0, - ,"22,874",388,100.0, - ,"22,183",414,100.0, - ,"18,168",300,100.0, - ,"25,490",376,100.0, - ,"29,283",415,100.0, - ,0,-,100.0, - ,"7,283",168,100.0, - ,"34,018",502,100.0, - ,"10,958",284,100.0, - ,"20,793",403,100.0, - ,162,48,100.0, - ,"26,309",478,100.0, - ,"8,352",206,100.0, - ,"16,388",343,100.0, - ,"11,533",345,100.0, - ,"6,264",214,100.0, - ,"36,495",527,100.0, - ,0,-,100.0, - ,"16,253",359,100.0, - ,"27,528",399,100.0, - ,"11,561",285,100.0, - ,"23,963",393,100.0, - ,"14,899",290,100.0, - ,"10,629",284,100.0, - ,"38,771",494,100.0, - ,"12,455",253,100.0, - ,"25,643",511,100.0, - ,"7,749",286,100.0, - ,"10,253",230,100.0, - ,"11,544",267,100.0, - ,"17,877",360,100.0, - ,"8,870",210,100.0, - ,"6,384",207,100.0, - ,"9,489",203,100.0, - ,"13,557",314,100.0, - ,"13,265",312,100.0, - ,"11,797",268,100.0, - ,"6,582",188,100.0, - ,"17,140",353,100.0, - ,"17,535",357,100.0, - ,"9,997",270,100.0, - ,"22,031",419,100.0, - ,"6,837",257,100.0, - ,"11,261",243,100.0, - ,"11,235",225,100.0, - ,"9,677",208,100.0, - ,"16,043",279,100.0, - ,"7,182",195,100.0, - ,"11,627",288,100.0, - ,"9,939",280,100.0, - ,"6,273",188,100.0, - ,"15,308",292,100.0, - ,"15,700",313,100.0, - ,"10,364",245,100.0, - ,"5,553",178,100.0, - ,"7,896",167,100.0, - ,"6,337",131,100.0, - ,"16,048",275,100.0, - ,"8,110",254,100.0, - ,"9,167",242,100.0, - ,"7,891",198,100.0, - ,"11,319",337,100.0, - ,"12,157",283,100.0, - ,"16,060",402,100.0, - ,"9,083",215,100.0, - ,"7,941",274,100.0, - ,"10,025",314,100.0, - ,"11,686",391,100.0, - ,"6,369",244,100.0, - ,"6,303",219,100.0, - ,"9,115",292,100.0, - ,"13,345",344,100.0, - ,"5,438",217,100.0, - ,"11,096",273,100.0, - ,"8,752",234,100.0, - ,"7,881",240,100.0, - ,"9,786",217,100.0, - ,"7,622",249,100.0, - ,"8,194",240,100.0, - ,"6,945",207,100.0, - ,"8,805",191,100.0, - ,"15,289",221,100.0, - ,-,-,-, - ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

Modified Dataset:
DESC (0)|NTACODE (1)|NTANAME (2)|***HOUSEHOLD BY TYPE (3)|TOTAL HOUSEHOLDS (4)|  FAMILY HOUSEHOLDS (FAMILIES) (5)|      WITH OWN CHILDREN UNDER 18 YEARS (6)|    MARRIED-COUPLE FAMILY (7)|      WITH OWN CHILDREN UNDER 18 YEARS (8)|    MALE HOUSEHLDR, NO WIFE PRESENT, FAMILY (9)|      WITH OWN CHILDREN UNDER 18 YEARS (10)|    FEMALE HOUSEHLDR, NO HUSBAND PRESENT, FAMILY (11)|      WITH OWN CHILDREN UNDER 18 YEARS (12)|  NONFAMILY HOUSEHOLDS (13)|    HOUSEHOLDER LIVING ALONE (14)|       65 YEARS AND OVER (15)|  HOUSEHOLDS WITH ONE OR MORE PEOPLE UNDER 18 YEARS (16)|  HOUSEHOLDS WITH ONE OR MORE PEOPLE 65 YEARS AND OVER (17)|  AVERAGE HOUSEHOLD SIZE (18)|  AVERAGE FAMILY SIZE (19)|***RELATIONSHIP (20)|POPULATION IN HOUSEHOLDS (21)|  HOUSEHOLDER (22)|  SPOUSE (23)|  CHILD (24)|  OTHER RELATIVES (25)|  NONRELATIVES (26)|  NONRELATIVES - UNMARRIED PARTNER (27)|***MARITAL STATUS (28)|MALES 15 YEARS AND OVER (29)|  NEVER MARRIED (30)|  NOW MARRIED, EXCEPT SEPARATED (31)|  SEPARATED (32)|  WIDOWED (33)|  DIVORCED (34)|FEMALES 15 YEARS AND OVER (35)|  NEVER MARRIED (36)|  NOW MARRIED, EXCEPT SEPARATED (37)|  SEPARATED (38)|  WIDOWED (39)|  DIVORCED (40)|***GRANDPARENTS (41)|  NUMBER OF GRANDPARENTS LIVING WITH OWN GRANDCHILDREN UNDER 18 YEARS (42)|  RESPONSIBLE FOR GRANDCHILDREN (43)|      LESS THAN 1 YEAR (44)|      1 OR 2 YEARS (45)|      3 OR 4 YEARS (46)|      5 OR MORE YEARS (47)|NUMBER OF GRANDPARENTS RESPONSIBLE FOR OWN GRANDCHILDREN UNDER 18 YEARS (48)|  WHO ARE FEMALE (49)|  WHO ARE MARRIED (50)|***SCHOOL ENROLLMENT (51)|POPULATION 3 YEARS AND OVER ENROLLED IN SCHOOL (52)|  NURSERY SCHOOL, PRESCHOOL (53)|  KINDERGARTEN (54)|  ELEMENTARY SCHOOL (GRADES 1-8) (55)|  HIGH SCHOOL (GRADES 9-12) (56)|  COLLEGE OR GRADUATE SCHOOL (57)|***EDUCATIONAL ATTAINMENT (58)|POPULATION 25 YEARS AND OVER (59)|  LESS THAN 9TH GRADE (60)|  9TH TO 12TH GRADE, NO DIPLOMA (61)|  HIGH SCHOOL GRADUATE (INCLUDES EQUIVALENCY) (62)|  SOME COLLEGE, NO DEGREE (63)|  ASSOCIATE'S DEGREE (64)|  BACHELOR'S DEGREE (65)|  GRADUATE OR PROFESSIONAL DEGREE (66)|  PERCENT HIGH SCHOOL GRADUATE OR HIGHER (67)|  PERCENT BACHELOR'S DEGREE OR HIGHER (68)|***VETERAN STATUS (69)|CIVILIAN POPULATION 18 YEARS AND OVER (70)|  CIVILIAN VETERANS (71)|***RESIDENCE 1 YEAR AGO (72)|POPULATION 1 YEAR AND OVER (73)|  SAME HOUSE (74)|  DIFFERENT HOUSE IN THE U.S. (75)|    SAME COUNTY (76)|    DIFFERENT COUNTY (77)|      SAME STATE (78)|      DIFFERENT STATE (79)|  ABROAD (80)|***PLACE OF BIRTH (81)|TOTAL POPULATION (82)|  NATIVE (83)|    BORN IN UNITED STATES (84)|      STATE OF RESIDENCE (85)|      DIFFERENT STATE (86)|    BORN IN PUERTO RICO, U.S. ISLAND AREAS, OR BORN ABROAD TO AMERICAN PARENT(S) (87)|  FOREIGN BORN (88)|***U.S. CITIZENSHIP STATUS (89)|FOREIGN-BORN POPULATION (90)|  NATURALIZED U.S. CITIZEN (91)|  NOT A U.S. CITIZEN (92)|***YEAR OF ENTRY (93)|POPULATION BORN OUTSIDE THE U.S. (94)|  NATIVE (95)|    ENTERED 2010 OR LATER (96)|    ENTERED BEFORE 2010 (97)|  FOREIGN BORN (98)|    ENTERED 2010 OR LATER (99)|    ENTERED BEFORE 2010 (100)|***WORLD REGION OF BIRTH OF FOREIGN BORN (101)|FOREIGN-BORN POPULATION, EXCLUDING POPULATION BORN AT SEA (102)|  EUROPE (103)|  ASIA (104)|  AFRICA (105)|  OCEANIA (106)|  LATIN AMERICA (107)|  NORTHERN AMERICA (108)|***LANGUAGE SPOKEN AT HOME (109)|POPULATION 5 YEARS AND OVER (110)|  ENGLISH ONLY (111)|  LANGUAGE OTHER THAN ENGLISH (112)|      SPEAK ENGLISH LESS THAN "VERY WELL" (113)|    SPANISH (114)|      SPEAK ENGLISH LESS THAN "VERY WELL" (115)|    OTHER INDO-EUROPEAN LANGUAGES (116)|      SPEAK ENGLISH LESS THAN "VERY WELL" (117)|    ASIAN AND PACIFIC ISLANDER LANGUAGES (118)|      SPEAK ENGLISH LESS THAN "VERY WELL" (119)|    OTHER LANGUAGES (120)|      SPEAK ENGLISH LESS THAN "VERY WELL" (121)|***ANCESTRY (122)|TOTAL POPULATION (123)|  AMERICAN (124)|  ARAB (125)|  CZECH (126)|  DANISH (127)|  DUTCH (128)|  ENGLISH (129)|  FRENCH (EXCEPT BASQUE) (130)|  FRENCH CANADIAN (131)|  GERMAN (132)|  GREEK (133)|  HUNGARIAN (134)|  IRISH (135)|  ITALIAN (136)|  LITHUANIAN (137)|  NORWEGIAN (138)|  POLISH (139)|  PORTUGUESE (140)|  RUSSIAN (141)|  SCOTCH-IRISH (142)|  SCOTTISH (143)|  SLOVAK (144)|  SUBSAHARAN AFRICAN (145)|  SWISS (146)|  UKRAINIAN (147)|  WELSH (148)|  WEST INDIAN (EXCLUDING HISPANIC ORIGIN GROUPS) (149)
DATA|BK72|Williamsburg|***HOUSEHOLD BY TYPE|8,299|6,813|4,855|5,640|4,170|389|218|784|467|1,486|1,359|881|5,026|1,642|3.9|4.5|***R
ELATIONSHIP|32,563|8,299|5,555|17,406|1,007|296|230|***MARITAL STATUS|9,451|2,948|5,962|82|276|183|9,821|2,624|5,831|277|804|285|*
**GRANDPARENTS|368|178|27|5|32|114|178|107|167|***SCHOOL ENROLLMENT|13,747|1,708|1,105|5,890|3,200|1,844|***EDUCATIONAL ATTAINMENT|12,929|1,889|2,398|4,915|2,244|483|577|423|-|-|***VETERAN STATUS|17,130|184|***RESIDENCE 1 YEAR AGO|31,730|30,889|786|617|169|158|11|55|***PLACE OF BIRTH|32,828|29,000|27,767|26,956|811|1,233|3,828|***U.S. CITIZENSHIP STATUS|3,828|2,584|1,244|***YEAR OF ENTRY|5,061|1,233|71|1,162|3,828|65|3,763|***WORLD REGION OF BIRTH OF FOREIGN BORN|3,828|1,708|598|114|8|1,147|253|***LANGUAGE SPOKEN AT HOME|26,665|3,126|23,539|8,895|3,476|1,671|19,063|6,795|36|12|964|417|***ANCESTRY|32,828|879|74|156|0|43|508|54|58|473|0|4,233|218|166|0|27|297|0|41|4|3|0|60|14|26|0|49
DATA|BK73|North Side-South Side|***HOUSEHOLD BY TYPE|19,172|8,691|3,628|5,161|2,294|876|350|2,654|984|10,481|6,048|1,270|4,219|2,720|2.4|3.3|***RELATIONSHIP|45,874|19,172|5,109|11,760|2,867|6,966|2,172|***MARITAL STATUS|18,487|10,931|6,024|263|259|1,010|19,771|10,611|5,711|836|959|1,654|***GRANDPARENTS|872|283|28|62|31|162|283|189|187|***SCHOOL ENROLLMENT|9,853|578|619|3,641|2,215|2,800|***EDUCATIONAL ATTAINMENT|31,774|3,418|3,086|4,676|4,218|1,205|9,827|5,344|-|-|***VETERAN STATUS|36,999|618|***RESIDENCE 1 YEAR AGO|45,458|39,289|5,725|2,687|3,038|1,879|1,159|444|***PLACE OF BIRTH|46,070|34,983|31,443|20,279|11,164|3,540|11,087|***U.S. CITIZENSHIP STATUS|11,087|4,902|6,185|***YEAR OF ENTRY|14,627|3,540|35|3,505|11,087|247|10,840|***WORLD REGION OF BIRTH OF FOREIGN BORN|11,087|2,786|1,544|57|123|6,222|355|***LANGUAGE SPOKEN AT HOME|43,053|18,702|24,351|9,996|15,404|6,762|7,316|2,636|1,146|430|485|168|***ANCESTRY|46,070|1,382|360|132|57|181|1,298|888|93|2,349|149|1,419|2,351|2,339|81|189|2,000|36|582|184|366|59|9|79|223|153|384


```

## 2- Add the Neighborhood Tabulation Area Dataset to the NY Crime Dataset
In this section, each crime record is matched to its neighborhood NTACode and NTADescription (ex:  BK72, Williamsburg), then appended to the crime file.  There are two options to generate this data;  either a Spark or Standalone job can be run.  



**Option 1) Run as Standalone**

When we first tried this, we tried to create a Spark job running on an Amazon EMR cluster.  The Spark job was successful when run on small test datasets of the crime data,  however stalled on larger datasets even though optimizations to the Spark job had been added.  Running the Standalone turned out to be the better and quicker option for us.  
```
cd create_crime_data

```

**Option 2) Run on Spark**

This was too slow even when run on our Amazon EMR cluster even after optimizing our code!  So, eventually we converted the Spark job to a standalone job.
```
cd create_crime_data

```


Thanks! 


