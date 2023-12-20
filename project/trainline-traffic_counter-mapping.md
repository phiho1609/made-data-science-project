# Mapping between trainlines and relevant traffic counters (_unfinished_)
This file shows a manual mapping between trainlines in Schleswig-Holstein (found in the train punctuality dataset) and location-wise relevant traffic counters (from the automatic traffic counter datasets from BASt).

The traffic counters are handpicked, based on a heuristic analysis of what roads might be taken as a substitute of one specific train line. Although automating this task certainly was a thought, the complexity seemed to high, since not only distance between position of a traffic counter and a trainline need to be used as input, but direction of a street, the connections to a street (is it even connected in a logical fashion to previous waypoints of the trainline) and travel time via that street. Such an algorithm would likely ease the process of picking traffic counters for a train line, but the development process seemed way to time-consuming and complex. Some algorithms of that sort might be available, but even their correct usage and implementation seemed more time consuming than handpicking the (limited) traffic counters. Consequently manual selection was preferred.

Following are the train lines found in the train punctuality dataset with their relevant traffic counters. They are split into 
\
_high interest_, meaning the measured street has a high likelyhood of being a substitute to the train line in question, and
\
_low interest_, meaning the measured street does seem somewhat plausible to be used as a substitute to the train line. 

## Itzehoe - Hamburg Hbf (RB 61)
#### High interest
- Nordoe 4 (1154; A23)
- Krupunder (1119; A23)
- Nordoe (Itzehoe) (1167; B77)
- Itzehoe-West 5 (1197; B206)


## Heide - Itzehoe (RB 62)
#### High interest
- Albersdorf (1270; A23)
- Schafstedt (1269; A23)
- Besdorf (1163; A23)
- Hademarschen (1268; A23)
- Itzehoe-West 5 (1197; B206)
- Hemmingstedt (1178; B5)
#### Low interest
- Süderholm (1125; B203)
- Brunsbüttel Hochbr. (1120; B5)


## Büsum - Neumünster (RB 63)
#### High interest
- Friedrichsgabekoog (1117; B203)
- Süderholm (1125; B203)
- Albersdorf (1270; A23)
- Schafstedt (1269; A23)
- Besdorf (1163; A23)
- Hademarschen (1268; A23)
- Hohenwestedt II (1187; B430)
- Lexfähre (1183; B203)


## St. Peter-Ording - Husum (RB 64)
#### High interest
- Kotzenbüll (1175; B202)
- Husum-Süd (1132; B5)
#### Low interest
- Tönning / Klappbrücke (1188; B5)


## Niebüll - Dagebüll (RB 65)
- - -


## Esbjerg - Niebüll (R / RB 66)
#### High interest
- Böglum (1133; B5)


## Wrist - Hamburg-Altona (RB 71)
#### High interest
- Moorkaten (1173; A7)
- Krupunder (1119; A23)
#### Low interest
- Elsensee (1144; B4)


## Eckernförde - Kiel (RB 73)
#### High interest
- Gettorf ( Wulfshagen ) (1116; B76)
#### Low interest
- Eckernförde / Goosefeld (1176; B203)
- Melsdorf (1162; A210)
- Kiel-West (1194; A215)
- Kiel-Holtenau I (1111; B503)
- Kiel-Holtenau II (1112; B503)


## Rendsburg - Kiel (RB 75???)
#### High interest
- Melsdorf (1162; A210)
- Kiel-West (1194; A215)
#### Low interest
- Rumohr (1104; A215)


## Schönberger Strand - Kiel (RB 76???)
#### High interest
- Kiel/Schönkirchen (1158; B502)
#### Low interest
- Raisdorf I (1135; B76)
- Raisdorf II (1136; B202)


## Bad Oldesloe - Hamburg (Hbf) (RB 81)
#### High interest
- Neritz (1190; B75)
- Barsbüttel (1102; A1)
#### Low interest
- Reinfeld (1291; A1)
- Bad Oldesloe (1101; A1)
- Bargteheide / Mollhagen (4701; B404)
- Grande (1185; B404)
- Glinde (SH) (1172; A24)
- Norderstedt (1118; B432)


## Neumünster - Bad Oldesloe (RB 82)
#### High interest
- Husberg (1186; B430)
- Segeberg West 2 (1166; A21)
#### Low interest
- AS Wankendorf (Stolpe) (1156; A21)
- Segeberg West I (1165; B206)
- Segeberg Ost (1164; B206)
- Reinfeld (1291; A1)
- Bad Oldesloe (1101; A1)


## Kiel - Lübeck (RB 84)
#### High interest
- Kiel-West (1194; A215)
- Rumohr (1104; A215)
- Einfeld (1106; A7)
- Segeberg West 1 (1165; B206)
- Segeberg West 2 (1166; A21)
- Segeberg Ost (1164; B206)
- Neddelsteenhof (1170; A20)
- Bad Schwartau (1108; A1)
- Raisdorf 1 (1135; B76)
- AS Wankendorf (Stolpe) (1156; A21)
- Röbel (1149; B76)
#### Low interest
- Husberg (1186; B430)


## Puttgarden - Lübeck (RB 85???)
#### High interest
- Fehmarnsundbrücke (1131; B207)
- Oldenburg (Holstein) (1124; A1)
- Neustadt i. H.-Süd (1105; A1)
- Bad Schwartau (1108; A1)
#### Low interest
- Neustadt i.H.-Ost (1192; B501)
- Döhnsdorf (1174; B202)
- Röbel (1149; B76)
- Untersteenrade (1151; B432)


## Travemünde - Lübeck (RB 86???)
#### High interest
- Bad Schwartau (1108; A1)


## Hamburg (Hbf) - Büchen (RE 1)
#### High interest
- Glinde (SH) (1172; A24)
- Neubörnsen (1138; B207)
#### Low interest
- Börnsen (1140; A25)
- Elmenhorst (1184; B207)


## Hamburg (Hbf) - Schwerin (RE 1)
#### High interest
- Glinde (SH) (1172; A24)
- Neubörnsen (1138; B207)
- Gudow (1110; A24)
- Pampow (1706; B321)
- Mustin (1160; B208)
#### Low interest
- Börnsen (1140; A25)
- Elmenhorst (1184; B207)
- Hagenow (1612; A24)