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

### Datasource1: Mobilithek
* Metadata URL: https://mobilithek.info/offers/-5903353853572013168
* Data URL: https://opendata.schleswig-holstein.de/dataset/84256bd9-562c-4ea0-b0c6-908cd1e9e593/resource/c1407750-f05f-4715-8688-c0ff01b49131/download/puenktlichkeit.csv
* Data Type: CSV

Monthly train punctuality score per train-line from 2010 to 2022

### Datasource2: BASt
* Metadata URL: https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/zaehl_node.html
* Data URLs: found at https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Daten/2010_1/Jawe2010.html?cms_map=1&cms_filter=true&cms_jahr=Jawe2017&cms_land=1&cms_strTyp=&cms_str=&cms_dtvKfz=&cms_dtvSv=
* Apparent overview of 2021 - 2023: https://www.bast.de/DE/Publikationen/Daten/Verkehrstechnik/DZ.html;jsessionid=CC5E94E17B4087DF370DA52A6A13A49C.live21301?nn=1954870
* Description of dataset values: https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/pdf-dateien/datensatzbeschreibung-Stundendaten.pdf?__blob=publicationFile&v=4
* Data Type: CSV (compressed)
* License: `Datenlizenz Deutschland Namensnennung 2.0` (see [here](https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Nutzungsbedingungen.html?nn=1819490) or on the previously mentioned overviews under _Hinweise zur Datennutzung_)
Contentwise analog datasources to Datasource2, but with varying location and year of recording

Hourly traffic count of categorized traffic on various roads

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Create data pipelines for the datasets [#7][i7]
2. Clean up data [#2][i2]
3. Decide on useful car traffic data sources [#1][i1]
4. Merge and compress car traffic data [#3][i3]
5. Create minimal comparison between train punctuality and car traffic count [#4][i4]
6. Create more complex comparisons [#5][i5]
7. Start working on the final report [#6][i6]

[i1]: https://github.com/phiho1609/made-data-science-project/issues/1
[i2]: https://github.com/phiho1609/made-data-science-project/issues/2
[i3]: https://github.com/phiho1609/made-data-science-project/issues/3
[i4]: https://github.com/phiho1609/made-data-science-project/issues/4
[i5]: https://github.com/phiho1609/made-data-science-project/issues/5
[i6]: https://github.com/phiho1609/made-data-science-project/issues/6
[i7]: https://github.com/phiho1609/made-data-science-project/issues/7
