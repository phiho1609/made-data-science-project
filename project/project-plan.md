# Project Plan

## Unpunctuality Tolerance of Schleswig-Holstein
<!-- Give your project a short title. -->
How much unpunctuality can germans take in?

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. Do people switch from train to car dependent on the Deutsche Bahn's punctuality? 
2. Did this behaviour change over time? 
## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
Unfortunately the german leader in railway transport, Deutsche Bahn, is infamous for its lack in punctuality. While this behaviour indeed does infuriate the germans, an interesting question would be at what level of unpunctuality, germans do consider switching from train to car. This project shall give insights into this possible consequence, by correlating unpunctuality of trains and the amount of car traffic in the german federal state of Schleswig-Holstein.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Train punctuality in Schleswig-Holstein
* Metadata URL: https://mobilithek.info/offers/-5903353853572013168
* Data URL: https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv
* Data Type: CSV

Monthly train punctuality score per train-line from 2010 to 2022

### Datasource2: Automatic traffic counter (A7 Moorkaten 2010)
* Metadata URL: https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/pdf-dateien/datensatzbeschreibung-Stundendaten.pdf?__blob=publicationFile&v=4
* Data URL: https://www.bast.de/videos/2010/zst1173.zip
* Data Type: CSV (compressed)

Hourly traffic count of categorized traffic on the A7 at Moorkaten throughout the year 2010

### Datasource3+: Automatic traffic counters (throughout Schleswig-Holstein, between 2010 - 2021)
* Metadata URL: https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/pdf-dateien/datensatzbeschreibung-Stundendaten.pdf?__blob=publicationFile&v=4
* Data URLs: found at https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Daten/2010_1/Jawe2010.html?cms_map=1&cms_filter=true&cms_jahr=Jawe2017&cms_land=1&cms_strTyp=&cms_str=&cms_dtvKfz=&cms_dtvSv=
* Data Type: CSV (compressed)

Contentwise analog datasources to Datasource2, but with varying location and year of recording

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Decide on useful car traffic data sources #1
2. Clean up data #2
3. Merge and compress car traffic data #3
4. Create minimal comparison between train punctuality and car traffic count #4
5. Create more complex comparisons #5
6. Start working on the final report #6
