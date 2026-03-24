All Men's Pakistan Super League match data in CSV format
========================================================

The background
--------------

As an experiment, after being asked by a user of the site, I started
converting the YAML data provided on the site into a CSV format. That
initial version was heavily influenced by the format used by the baseball
project Retrosheet. I wasn't sure of the usefulness of my CSV format, but
nothing better was suggested so I persisted with it. Later Ashwin Raman
(https://twitter.com/AshwinRaman_) send me a detailed example of a format
he felt might work and, liking what I saw, I started to produce data in
a slightly modified version of that initial example.

This particular zip folder contains the CSV data for...
  All Men's Pakistan Super League matches
...for which we have data.

How you can help
----------------

Providing feedback on the data would be the most helpful. Tell me what you
like and what you don't. Is there anything that is in the JSON data that
you'd like to be included in the CSV? Could something be included in a better
format? General views and comments help, as well as incredibly detailed
feedback. All information is of use to me at this stage. I can only improve
the data if people tell me what does works and what doesn't. I'd like to make
the data as useful as possible but I need your help to do it. Also, which of
the 2 CSV formats do you prefer, this one or the original? Ideally I'd like
to settle on a single CSV format so what should be kept from each?

Finally, any feedback as to the licence the data should be released under
would be greatly appreciated. Licensing is a strange little world and I'd
like to choose the "right" licence. My basic criteria may be that:

  * the data should be free,
  * corrections are encouraged/required to be reported to the project,
  * derivative works are allowed,
  * you can't just take data and sell it.

Feedback, pointers, comments, etc on licensing are welcome.

The format of the data
----------------------

Full documentation of this CSV format can be found at:
  https://cricsheet.org/format/csv_ashwin/
but the following is a brief summary of the details...

This format consists of 2 files per match, although you can get all of
the ball-by-ball data from just one of the files. The files for a match
are named <id>.csv (for the ball-by-ball data), and <id>_info.csv (for
the match info), where <id> is the Cricinfo id for the match. The
ball-by-ball file contains one row per delivery in the match, while the
match info file contains match information such as dates the match was
played, the outcome, and lists of the players involved in the match.

The match info file format
--------------------------

The info section contains the information on the actual match, such as
when and where it was played, any event it was part of, the type of
match etc. The fields included in the info section will each appear as
one or more rows in the data. Some of the fields are required, whereas
some are optional. If a field has multiple values, such as team, then
each value will appear on a row of it's own.

The ball-by-ball file format
----------------------------

The first row of each ball-by-ball CSV file contains the headers for the
file, with each subsequent row providing details on a single delivery.
The headers in the file are:

  * match_id
  * season
  * start_date
  * venue
  * innings
  * ball
  * batting_team
  * bowling_team
  * striker
  * non_striker
  * bowler
  * runs_off_bat
  * extras
  * wides
  * noballs
  * byes
  * legbyes
  * penalty
  * wicket_type
  * player_dismissed
  * other_wicket_type
  * other_player_dismissed

Most of the fields above should, hopefully, be self-explanatory, but some may
benefit from clarification...

"innings" contains the number of the innings within the match. If a match is
one that would normally have 2 innings, such as a T20 or ODI, then any innings
of more than 2 can be regarded as a super over.

"ball" is a combination of the over and delivery. For example, "0.3" represents
the 3rd ball of the 1st over.

"wides", "noballs", "byes", "legbyes", and "penalty" contain the total of each
particular type of extras, or are blank if not relevant to the delivery.

If a wicket occurred on a delivery then "wicket_type" will contain the method
of dismissal, while "player_dismissed" will indicate who was dismissed. There
is also the, admittedly remote, possibility that a second dismissal can be
recorded on the delivery (such as when a player retires on the same delivery
as another dismissal occurs). In this case "other_wicket_type" will record
the reason, while "other_player_dismissed" will show who was dismissed.

Matches included in this archive
--------------------------------

2025-05-25 - club - PSL - male - 1475271 - Quetta Gladiators vs Lahore Qalandars
2025-05-23 - club - PSL - male - 1475270 - Lahore Qalandars vs Islamabad United
2025-05-22 - club - PSL - male - 1475269 - Karachi Kings vs Lahore Qalandars
2025-05-21 - club - PSL - male - 1475268 - Quetta Gladiators vs Islamabad United
2025-05-19 - club - PSL - male - 1475267 - Islamabad United vs Karachi Kings
2025-05-18 - club - PSL - male - 1475266 - Lahore Qalandars vs Peshawar Zalmi
2025-05-18 - club - PSL - male - 1475265 - Multan Sultans vs Quetta Gladiators
2025-05-17 - club - PSL - male - 1475264 - Karachi Kings vs Peshawar Zalmi
2025-05-07 - club - PSL - male - 1475263 - Quetta Gladiators vs Islamabad United
2025-05-05 - club - PSL - male - 1475262 - Multan Sultans vs Peshawar Zalmi
2025-05-04 - club - PSL - male - 1475261 - Lahore Qalandars vs Karachi Kings
2025-05-03 - club - PSL - male - 1475260 - Islamabad United vs Quetta Gladiators
2025-05-02 - club - PSL - male - 1475259 - Islamabad United vs Peshawar Zalmi
2025-05-01 - club - PSL - male - 1475258 - Lahore Qalandars vs Quetta Gladiators
2025-05-01 - club - PSL - male - 1475257 - Karachi Kings vs Multan Sultans
2025-04-30 - club - PSL - male - 1475256 - Lahore Qalandars vs Islamabad United
2025-04-29 - club - PSL - male - 1475255 - Multan Sultans vs Quetta Gladiators
2025-04-27 - club - PSL - male - 1475254 - Quetta Gladiators vs Peshawar Zalmi
2025-04-26 - club - PSL - male - 1475253 - Multan Sultans vs Lahore Qalandars
2025-04-25 - club - PSL - male - 1475252 - Quetta Gladiators vs Karachi Kings
2025-04-24 - club - PSL - male - 1475251 - Lahore Qalandars vs Peshawar Zalmi
2025-04-23 - club - PSL - male - 1475250 - Multan Sultans vs Islamabad United
2025-04-22 - club - PSL - male - 1475249 - Multan Sultans vs Lahore Qalandars
2025-04-21 - club - PSL - male - 1475248 - Peshawar Zalmi vs Karachi Kings
2025-04-20 - club - PSL - male - 1475247 - Karachi Kings vs Islamabad United
2025-04-19 - club - PSL - male - 1475246 - Peshawar Zalmi vs Multan Sultans
2025-04-18 - club - PSL - male - 1475245 - Karachi Kings vs Quetta Gladiators
2025-04-16 - club - PSL - male - 1475244 - Islamabad United vs Multan Sultans
2025-04-15 - club - PSL - male - 1475243 - Lahore Qalandars vs Karachi Kings
2025-04-14 - club - PSL - male - 1475242 - Islamabad United vs Peshawar Zalmi
2025-04-13 - club - PSL - male - 1475241 - Lahore Qalandars vs Quetta Gladiators
2025-04-12 - club - PSL - male - 1475240 - Multan Sultans vs Karachi Kings
2025-04-12 - club - PSL - male - 1475239 - Quetta Gladiators vs Peshawar Zalmi
2025-04-11 - club - PSL - male - 1475238 - Lahore Qalandars vs Islamabad United
2024-03-18 - club - PSL - male - 1416505 - Multan Sultans vs Islamabad United
2024-03-16 - club - PSL - male - 1416504 - Peshawar Zalmi vs Islamabad United
2024-03-15 - club - PSL - male - 1416503 - Islamabad United vs Quetta Gladiators
2024-03-14 - club - PSL - male - 1416502 - Peshawar Zalmi vs Multan Sultans
2024-03-12 - club - PSL - male - 1416501 - Multan Sultans vs Quetta Gladiators
2024-03-11 - club - PSL - male - 1416500 - Peshawar Zalmi vs Karachi Kings
2024-03-10 - club - PSL - male - 1416499 - Lahore Qalandars vs Quetta Gladiators
2024-03-10 - club - PSL - male - 1416498 - Multan Sultans vs Islamabad United
2024-03-09 - club - PSL - male - 1416497 - Lahore Qalandars vs Karachi Kings
2024-03-08 - club - PSL - male - 1416496 - Peshawar Zalmi vs Quetta Gladiators
2024-03-07 - club - PSL - male - 1416495 - Karachi Kings vs Islamabad United
2024-03-06 - club - PSL - male - 1416494 - Lahore Qalandars vs Islamabad United
2024-03-06 - club - PSL - male - 1416493 - Quetta Gladiators vs Karachi Kings
2024-03-05 - club - PSL - male - 1416492 - Peshawar Zalmi vs Multan Sultans
2024-03-04 - club - PSL - male - 1416491 - Islamabad United vs Peshawar Zalmi
2024-03-03 - club - PSL - male - 1416490 - Multan Sultans vs Karachi Kings
2024-02-29 - club - PSL - male - 1416487 - Karachi Kings vs Quetta Gladiators
2024-02-28 - club - PSL - male - 1416486 - Karachi Kings vs Islamabad United
2024-02-27 - club - PSL - male - 1416485 - Multan Sultans vs Lahore Qalandars
2024-02-26 - club - PSL - male - 1416484 - Peshawar Zalmi vs Islamabad United
2024-02-25 - club - PSL - male - 1416483 - Peshawar Zalmi vs Lahore Qalandars
2024-02-25 - club - PSL - male - 1416482 - Multan Sultans vs Quetta Gladiators
2024-02-24 - club - PSL - male - 1416481 - Lahore Qalandars vs Karachi Kings
2024-02-23 - club - PSL - male - 1416480 - Peshawar Zalmi vs Multan Sultans
2024-02-22 - club - PSL - male - 1416479 - Islamabad United vs Quetta Gladiators
2024-02-21 - club - PSL - male - 1416478 - Lahore Qalandars vs Multan Sultans
2024-02-21 - club - PSL - male - 1416477 - Peshawar Zalmi vs Karachi Kings
2024-02-20 - club - PSL - male - 1416476 - Islamabad United vs Multan Sultans
2024-02-19 - club - PSL - male - 1416475 - Lahore Qalandars vs Quetta Gladiators
2024-02-18 - club - PSL - male - 1416474 - Multan Sultans vs Karachi Kings
2024-02-18 - club - PSL - male - 1416473 - Quetta Gladiators vs Peshawar Zalmi
2024-02-17 - club - PSL - male - 1416472 - Lahore Qalandars vs Islamabad United
2023-03-18 - club - PSL - male - 1354957 - Lahore Qalandars vs Multan Sultans
2023-03-17 - club - PSL - male - 1354956 - Peshawar Zalmi vs Lahore Qalandars
2023-03-16 - club - PSL - male - 1354955 - Peshawar Zalmi vs Islamabad United
2023-03-15 - club - PSL - male - 1354954 - Multan Sultans vs Lahore Qalandars
2023-03-12 - club - PSL - male - 1354953 - Karachi Kings vs Lahore Qalandars
2023-03-12 - club - PSL - male - 1354952 - Peshawar Zalmi vs Islamabad United
2023-03-11 - club - PSL - male - 1354951 - Multan Sultans vs Quetta Gladiators
2023-03-10 - club - PSL - male - 1354950 - Peshawar Zalmi vs Multan Sultans
2023-03-09 - club - PSL - male - 1354949 - Lahore Qalandars vs Islamabad United
2023-03-08 - club - PSL - male - 1354948 - Peshawar Zalmi vs Quetta Gladiators
2023-03-07 - club - PSL - male - 1354947 - Multan Sultans vs Islamabad United
2023-03-07 - club - PSL - male - 1354946 - Peshawar Zalmi vs Lahore Qalandars
2023-03-06 - club - PSL - male - 1354945 - Karachi Kings vs Quetta Gladiators
2023-03-05 - club - PSL - male - 1354944 - Quetta Gladiators vs Islamabad United
2023-03-04 - club - PSL - male - 1354943 - Lahore Qalandars vs Multan Sultans
2023-03-03 - club - PSL - male - 1354942 - Karachi Kings vs Islamabad United
2023-03-02 - club - PSL - male - 1354941 - Lahore Qalandars vs Quetta Gladiators
2023-03-01 - club - PSL - male - 1354940 - Peshawar Zalmi vs Karachi Kings
2023-02-27 - club - PSL - male - 1354939 - Lahore Qalandars vs Islamabad United
2023-02-26 - club - PSL - male - 1354938 - Lahore Qalandars vs Peshawar Zalmi
2023-02-26 - club - PSL - male - 1354937 - Karachi Kings vs Multan Sultans
2023-02-24 - club - PSL - male - 1354936 - Islamabad United vs Quetta Gladiators
2023-02-23 - club - PSL - male - 1354935 - Peshawar Zalmi vs Islamabad United
2023-02-22 - club - PSL - male - 1354934 - Multan Sultans vs Karachi Kings
2023-02-21 - club - PSL - male - 1354933 - Lahore Qalandars vs Quetta Gladiators
2023-02-20 - club - PSL - male - 1354932 - Quetta Gladiators vs Peshawar Zalmi
2023-02-19 - club - PSL - male - 1354931 - Karachi Kings vs Lahore Qalandars
2023-02-19 - club - PSL - male - 1354930 - Multan Sultans vs Islamabad United
2023-02-18 - club - PSL - male - 1354929 - Quetta Gladiators vs Karachi Kings
2023-02-17 - club - PSL - male - 1354928 - Multan Sultans vs Peshawar Zalmi
2023-02-16 - club - PSL - male - 1354927 - Karachi Kings vs Islamabad United
2023-02-15 - club - PSL - male - 1354926 - Quetta Gladiators vs Multan Sultans
2023-02-14 - club - PSL - male - 1354925 - Peshawar Zalmi vs Karachi Kings
2023-02-13 - club - PSL - male - 1354924 - Lahore Qalandars vs Multan Sultans
2022-02-27 - club - PSL - male - 1293033 - Lahore Qalandars vs Multan Sultans
2022-02-25 - club - PSL - male - 1293032 - Lahore Qalandars vs Islamabad United
2022-02-24 - club - PSL - male - 1293031 - Peshawar Zalmi vs Islamabad United
2022-02-23 - club - PSL - male - 1293030 - Multan Sultans vs Lahore Qalandars
2022-02-21 - club - PSL - male - 1293029 - Peshawar Zalmi vs Lahore Qalandars
2022-02-20 - club - PSL - male - 1293028 - Islamabad United vs Multan Sultans
2022-02-20 - club - PSL - male - 1293027 - Quetta Gladiators vs Karachi Kings
2022-02-19 - club - PSL - male - 1293026 - Lahore Qalandars vs Islamabad United
2022-02-18 - club - PSL - male - 1293025 - Karachi Kings vs Lahore Qalandars
2022-02-18 - club - PSL - male - 1293024 - Multan Sultans vs Quetta Gladiators
2022-02-17 - club - PSL - male - 1293023 - Peshawar Zalmi vs Islamabad United
2022-02-16 - club - PSL - male - 1293022 - Karachi Kings vs Multan Sultans
2022-02-15 - club - PSL - male - 1293021 - Peshawar Zalmi vs Quetta Gladiators
2022-02-14 - club - PSL - male - 1293020 - Islamabad United vs Karachi Kings
2022-02-13 - club - PSL - male - 1293019 - Quetta Gladiators vs Lahore Qalandars
2022-02-13 - club - PSL - male - 1293018 - Peshawar Zalmi vs Karachi Kings
2022-02-12 - club - PSL - male - 1293017 - Islamabad United vs Quetta Gladiators
2022-02-11 - club - PSL - male - 1293016 - Lahore Qalandars vs Multan Sultans
2022-02-10 - club - PSL - male - 1293015 - Multan Sultans vs Peshawar Zalmi
2022-02-07 - club - PSL - male - 1293014 - Lahore Qalandars vs Quetta Gladiators
2022-02-06 - club - PSL - male - 1293013 - Islamabad United vs Karachi Kings
2022-02-05 - club - PSL - male - 1293012 - Multan Sultans vs Peshawar Zalmi
2022-02-05 - club - PSL - male - 1293011 - Lahore Qalandars vs Islamabad United
2022-02-04 - club - PSL - male - 1293010 - Peshawar Zalmi vs Karachi Kings
2022-02-03 - club - PSL - male - 1293009 - Islamabad United vs Quetta Gladiators
2022-02-02 - club - PSL - male - 1293008 - Lahore Qalandars vs Peshawar Zalmi
2022-02-01 - club - PSL - male - 1293007 - Multan Sultans vs Islamabad United
2022-01-31 - club - PSL - male - 1293006 - Multan Sultans vs Quetta Gladiators
2022-01-30 - club - PSL - male - 1293005 - Karachi Kings vs Lahore Qalandars
2022-01-30 - club - PSL - male - 1293004 - Peshawar Zalmi vs Islamabad United
2022-01-29 - club - PSL - male - 1293003 - Karachi Kings vs Quetta Gladiators
2022-01-29 - club - PSL - male - 1293002 - Lahore Qalandars vs Multan Sultans
2022-01-28 - club - PSL - male - 1293001 - Quetta Gladiators vs Peshawar Zalmi
2022-01-27 - club - PSL - male - 1293000 - Karachi Kings vs Multan Sultans
2021-06-24 - club - PSL - male - 1247044 - Multan Sultans vs Peshawar Zalmi
2021-06-22 - club - PSL - male - 1247043 - Islamabad United vs Peshawar Zalmi
2021-06-21 - club - PSL - male - 1247042 - Karachi Kings vs Peshawar Zalmi
2021-06-21 - club - PSL - male - 1247041 - Multan Sultans vs Islamabad United
2021-06-19 - club - PSL - male - 1247040 - Multan Sultans vs Islamabad United
2021-06-19 - club - PSL - male - 1247039 - Karachi Kings vs Quetta Gladiators
2021-06-18 - club - PSL - male - 1247038 - Multan Sultans vs Lahore Qalandars
2021-06-17 - club - PSL - male - 1247037 - Karachi Kings vs Lahore Qalandars
2021-06-17 - club - PSL - male - 1247036 - Islamabad United vs Peshawar Zalmi
2021-06-16 - club - PSL - male - 1247035 - Multan Sultans vs Quetta Gladiators
2021-06-15 - club - PSL - male - 1247034 - Karachi Kings vs Peshawar Zalmi
2021-06-15 - club - PSL - male - 1247033 - Quetta Gladiators vs Lahore Qalandars
2021-06-14 - club - PSL - male - 1247032 - Karachi Kings vs Islamabad United
2021-06-13 - club - PSL - male - 1247031 - Peshawar Zalmi vs Multan Sultans
2021-06-13 - club - PSL - male - 1247029 - Islamabad United vs Lahore Qalandars
2021-06-12 - club - PSL - male - 1247030 - Peshawar Zalmi vs Quetta Gladiators
2021-06-11 - club - PSL - male - 1247028 - Quetta Gladiators vs Islamabad United
2021-06-10 - club - PSL - male - 1247026 - Lahore Qalandars vs Peshawar Zalmi
2021-06-10 - club - PSL - male - 1247025 - Multan Sultans vs Karachi Kings
2021-06-09 - club - PSL - male - 1247027 - Islamabad United vs Lahore Qalandars
2021-03-03 - club - PSL - male - 1247024 - Quetta Gladiators vs Multan Sultans
2021-03-03 - club - PSL - male - 1247023 - Peshawar Zalmi vs Karachi Kings
2021-03-02 - club - PSL - male - 1247022 - Quetta Gladiators vs Islamabad United
2021-02-28 - club - PSL - male - 1247021 - Karachi Kings vs Lahore Qalandars
2021-02-27 - club - PSL - male - 1247020 - Islamabad United vs Peshawar Zalmi
2021-02-27 - club - PSL - male - 1247019 - Multan Sultans vs Karachi Kings
2021-02-26 - club - PSL - male - 1247018 - Quetta Gladiators vs Peshawar Zalmi
2021-02-26 - club - PSL - male - 1247017 - Lahore Qalandars vs Multan Sultans
2021-02-24 - club - PSL - male - 1247016 - Karachi Kings vs Islamabad United
2021-02-23 - club - PSL - male - 1247015 - Multan Sultans vs Peshawar Zalmi
2021-02-22 - club - PSL - male - 1247014 - Quetta Gladiators vs Lahore Qalandars
2021-02-21 - club - PSL - male - 1247013 - Multan Sultans vs Islamabad United
2021-02-21 - club - PSL - male - 1247012 - Peshawar Zalmi vs Lahore Qalandars
2021-02-20 - club - PSL - male - 1247011 - Quetta Gladiators vs Karachi Kings
2020-11-17 - club - PSL - male - 1231146 - Lahore Qalandars vs Karachi Kings
2020-11-15 - club - PSL - male - 1211675 - Lahore Qalandars vs Multan Sultans
2020-11-14 - club - PSL - male - 1211673 - Peshawar Zalmi vs Lahore Qalandars
2020-11-14 - club - PSL - male - 1211672 - Multan Sultans vs Karachi Kings
2020-03-15 - club - PSL - male - 1211671 - Karachi Kings vs Quetta Gladiators
2020-03-15 - club - PSL - male - 1211670 - Lahore Qalandars vs Multan Sultans
2020-03-14 - club - PSL - male - 1211669 - Karachi Kings vs Islamabad United
2020-03-13 - club - PSL - male - 1211668 - Multan Sultans vs Peshawar Zalmi
2020-03-12 - club - PSL - male - 1211667 - Karachi Kings vs Lahore Qalandars
2020-03-10 - club - PSL - male - 1211665 - Lahore Qalandars vs Peshawar Zalmi
2020-03-08 - club - PSL - male - 1211664 - Lahore Qalandars vs Karachi Kings
2020-03-08 - club - PSL - male - 1211663 - Islamabad United vs Multan Sultans
2020-03-07 - club - PSL - male - 1211662 - Lahore Qalandars vs Quetta Gladiators
2020-03-07 - club - PSL - male - 1211661 - Islamabad United vs Peshawar Zalmi
2020-03-06 - club - PSL - male - 1211660 - Karachi Kings vs Multan Sultans
2020-03-05 - club - PSL - male - 1211659 - Peshawar Zalmi vs Quetta Gladiators
2020-03-04 - club - PSL - male - 1211658 - Lahore Qalandars vs Islamabad United
2020-03-03 - club - PSL - male - 1211657 - Lahore Qalandars vs Quetta Gladiators
2020-03-02 - club - PSL - male - 1211656 - Karachi Kings vs Peshawar Zalmi
2020-03-01 - club - PSL - male - 1211655 - Islamabad United vs Karachi Kings
2020-02-29 - club - PSL - male - 1211653 - Multan Sultans vs Quetta Gladiators
2020-02-28 - club - PSL - male - 1211652 - Lahore Qalandars vs Peshawar Zalmi
2020-02-28 - club - PSL - male - 1211651 - Multan Sultans vs Karachi Kings
2020-02-27 - club - PSL - male - 1211650 - Islamabad United vs Quetta Gladiators
2020-02-26 - club - PSL - male - 1211649 - Multan Sultans vs Peshawar Zalmi
2020-02-23 - club - PSL - male - 1211648 - Lahore Qalandars vs Islamabad United
2020-02-23 - club - PSL - male - 1211647 - Karachi Kings vs Quetta Gladiators
2020-02-22 - club - PSL - male - 1211646 - Islamabad United vs Multan Sultans
2020-02-22 - club - PSL - male - 1211645 - Peshawar Zalmi vs Quetta Gladiators
2020-02-21 - club - PSL - male - 1211644 - Lahore Qalandars vs Multan Sultans
2020-02-21 - club - PSL - male - 1211643 - Karachi Kings vs Peshawar Zalmi
2020-02-20 - club - PSL - male - 1211642 - Islamabad United vs Quetta Gladiators
2019-03-17 - club - PSL - male - 1168853 - Quetta Gladiators vs Peshawar Zalmi
2019-03-15 - club - PSL - male - 1168852 - Islamabad United vs Peshawar Zalmi
2019-03-14 - club - PSL - male - 1168850 - Islamabad United vs Karachi Kings
2019-03-13 - club - PSL - male - 1168851 - Peshawar Zalmi vs Quetta Gladiators
2019-03-11 - club - PSL - male - 1168849 - Lahore Qalandars vs Multan Sultans
2019-03-11 - club - PSL - male - 1168846 - Karachi Kings vs Peshawar Zalmi
2019-03-10 - club - PSL - male - 1168848 - Karachi Kings vs Quetta Gladiators
2019-03-09 - club - PSL - male - 1168847 - Lahore Qalandars vs Islamabad United
2019-03-05 - club - PSL - male - 1168845 - Islamabad United vs Quetta Gladiators
2019-03-05 - club - PSL - male - 1168844 - Lahore Qalandars vs Peshawar Zalmi
2019-03-04 - club - PSL - male - 1168843 - Karachi Kings vs Multan Sultans
2019-03-04 - club - PSL - male - 1168842 - Peshawar Zalmi vs Quetta Gladiators
2019-03-01 - club - PSL - male - 1168841 - Multan Sultans vs Quetta Gladiators
2019-03-01 - club - PSL - male - 1168840 - Islamabad United vs Peshawar Zalmi
2019-02-28 - club - PSL - male - 1168839 - Karachi Kings vs Lahore Qalandars
2019-02-28 - club - PSL - male - 1168838 - Multan Sultans vs Peshawar Zalmi
2019-02-27 - club - PSL - male - 1168837 - Islamabad United vs Karachi Kings
2019-02-27 - club - PSL - male - 1168836 - Lahore Qalandars vs Quetta Gladiators
2019-02-26 - club - PSL - male - 1168835 - Islamabad United vs Multan Sultans
2019-02-24 - club - PSL - male - 1168834 - Karachi Kings vs Quetta Gladiators
2019-02-24 - club - PSL - male - 1168833 - Multan Sultans vs Peshawar Zalmi
2019-02-23 - club - PSL - male - 1168832 - Islamabad United vs Karachi Kings
2019-02-23 - club - PSL - male - 1168831 - Lahore Qalandars vs Quetta Gladiators
2019-02-22 - club - PSL - male - 1168830 - Islamabad United vs Peshawar Zalmi
2019-02-22 - club - PSL - male - 1168829 - Lahore Qalandars vs Multan Sultans
2019-02-21 - club - PSL - male - 1168828 - Karachi Kings vs Peshawar Zalmi
2019-02-20 - club - PSL - male - 1168827 - Multan Sultans vs Quetta Gladiators
2019-02-17 - club - PSL - male - 1168826 - Lahore Qalandars vs Peshawar Zalmi
2019-02-17 - club - PSL - male - 1168825 - Islamabad United vs Quetta Gladiators
2019-02-16 - club - PSL - male - 1168824 - Karachi Kings vs Lahore Qalandars
2019-02-16 - club - PSL - male - 1168823 - Islamabad United vs Multan Sultans
2019-02-15 - club - PSL - male - 1168822 - Peshawar Zalmi vs Quetta Gladiators
2019-02-15 - club - PSL - male - 1168821 - Karachi Kings vs Multan Sultans
2019-02-14 - club - PSL - male - 1168820 - Islamabad United vs Lahore Qalandars
2018-03-25 - club - PSL - male - 1128851 - Islamabad United vs Peshawar Zalmi
2018-03-21 - club - PSL - male - 1128850 - Karachi Kings vs Peshawar Zalmi
2018-03-20 - club - PSL - male - 1128849 - Peshawar Zalmi vs Quetta Gladiators
2018-03-18 - club - PSL - male - 1128848 - Islamabad United vs Karachi Kings
2018-03-16 - club - PSL - male - 1128847 - Islamabad United vs Karachi Kings
2018-03-16 - club - PSL - male - 1128846 - Peshawar Zalmi vs Lahore Qalandars
2018-03-15 - club - PSL - male - 1128845 - Quetta Gladiators vs Islamabad United
2018-03-15 - club - PSL - male - 1128844 - Peshawar Zalmi vs Karachi Kings
2018-03-14 - club - PSL - male - 1128843 - Quetta Gladiators vs Lahore Qalandars
2018-03-13 - club - PSL - male - 1128842 - Multan Sultans vs Islamabad United
2018-03-11 - club - PSL - male - 1128841 - Karachi Kings vs Lahore Qalandars
2018-03-10 - club - PSL - male - 1128840 - Peshawar Zalmi vs Quetta Gladiators
2018-03-10 - club - PSL - male - 1128839 - Multan Sultans vs Karachi Kings
2018-03-09 - club - PSL - male - 1128838 - Peshawar Zalmi vs Islamabad United
2018-03-09 - club - PSL - male - 1128837 - Multan Sultans vs Lahore Qalandars
2018-03-08 - club - PSL - male - 1128836 - Karachi Kings vs Quetta Gladiators
2018-03-08 - club - PSL - male - 1128835 - Islamabad United vs Lahore Qalandars
2018-03-07 - club - PSL - male - 1128834 - Multan Sultans vs Quetta Gladiators
2018-03-06 - club - PSL - male - 1128833 - Peshawar Zalmi vs Multan Sultans
2018-03-04 - club - PSL - male - 1128832 - Islamabad United vs Karachi Kings
2018-03-03 - club - PSL - male - 1128831 - Peshawar Zalmi vs Lahore Qalandars
2018-03-03 - club - PSL - male - 1128830 - Multan Sultans vs Quetta Gladiators
2018-03-02 - club - PSL - male - 1128829 - Lahore Qalandars vs Islamabad United
2018-03-01 - club - PSL - male - 1128827 - Quetta Gladiators vs Peshawar Zalmi
2018-02-28 - club - PSL - male - 1128826 - Islamabad United vs Quetta Gladiators
2018-02-26 - club - PSL - male - 1128825 - Karachi Kings vs Lahore Qalandars
2018-02-25 - club - PSL - male - 1128824 - Karachi Kings vs Peshawar Zalmi
2018-02-25 - club - PSL - male - 1128823 - Multan Sultans vs Islamabad United
2018-02-24 - club - PSL - male - 1128822 - Quetta Gladiators vs Lahore Qalandars
2018-02-24 - club - PSL - male - 1128821 - Islamabad United vs Peshawar Zalmi
2018-02-23 - club - PSL - male - 1128820 - Multan Sultans vs Lahore Qalandars
2018-02-23 - club - PSL - male - 1128819 - Karachi Kings vs Quetta Gladiators
2018-02-22 - club - PSL - male - 1128818 - Peshawar Zalmi vs Multan Sultans
2017-03-05 - club - PSL - male - 1076009 - Peshawar Zalmi vs Quetta Gladiators
2017-03-03 - club - PSL - male - 1076008 - Peshawar Zalmi vs Karachi Kings
2017-03-01 - club - PSL - male - 1076007 - Karachi Kings vs Islamabad United
2017-02-28 - club - PSL - male - 1076006 - Quetta Gladiators vs Peshawar Zalmi
2017-02-26 - club - PSL - male - 1076005 - Islamabad United vs Karachi Kings
2017-02-25 - club - PSL - male - 1076004 - Peshawar Zalmi vs Quetta Gladiators
2017-02-25 - club - PSL - male - 1076003 - Karachi Kings vs Lahore Qalandars
2017-02-24 - club - PSL - male - 1076002 - Islamabad United vs Quetta Gladiators
2017-02-24 - club - PSL - male - 1076001 - Lahore Qalandars vs Peshawar Zalmi
2017-02-23 - club - PSL - male - 1076000 - Karachi Kings vs Quetta Gladiators
2017-02-20 - club - PSL - male - 1075999 - Islamabad United vs Lahore Qalandars
2017-02-19 - club - PSL - male - 1075998 - Karachi Kings vs Peshawar Zalmi
2017-02-18 - club - PSL - male - 1075997 - Islamabad United vs Peshawar Zalmi
2017-02-18 - club - PSL - male - 1075996 - Lahore Qalandars vs Quetta Gladiators
2017-02-17 - club - PSL - male - 1075995 - Islamabad United vs Karachi Kings
2017-02-17 - club - PSL - male - 1075994 - Peshawar Zalmi vs Quetta Gladiators
2017-02-16 - club - PSL - male - 1075993 - Karachi Kings vs Lahore Qalandars
2017-02-15 - club - PSL - male - 1075992 - Islamabad United vs Quetta Gladiators
2017-02-12 - club - PSL - male - 1075991 - Lahore Qalandars vs Peshawar Zalmi
2017-02-11 - club - PSL - male - 1075990 - Karachi Kings vs Quetta Gladiators
2017-02-11 - club - PSL - male - 1075989 - Islamabad United vs Lahore Qalandars
2017-02-10 - club - PSL - male - 1075988 - Karachi Kings vs Peshawar Zalmi
2017-02-10 - club - PSL - male - 1075987 - Lahore Qalandars vs Quetta Gladiators
2017-02-09 - club - PSL - male - 1075986 - Islamabad United vs Peshawar Zalmi
2016-02-23 - club - PSL - male - 959221 - Quetta Gladiators vs Islamabad United
2016-02-21 - club - PSL - male - 959219 - Islamabad United vs Peshawar Zalmi
2016-02-20 - club - PSL - male - 959217 - Karachi Kings vs Islamabad United
2016-02-19 - club - PSL - male - 959215 - Quetta Gladiators vs Peshawar Zalmi
2016-02-17 - club - PSL - male - 959213 - Lahore Qalandars vs Islamabad United
2016-02-17 - club - PSL - male - 959211 - Karachi Kings vs Peshawar Zalmi
2016-02-16 - club - PSL - male - 959209 - Lahore Qalandars vs Quetta Gladiators
2016-02-14 - club - PSL - male - 959207 - Quetta Gladiators vs Peshawar Zalmi
2016-02-14 - club - PSL - male - 959205 - Karachi Kings vs Islamabad United
2016-02-13 - club - PSL - male - 959203 - Lahore Qalandars vs Peshawar Zalmi
2016-02-13 - club - PSL - male - 959201 - Karachi Kings vs Quetta Gladiators
2016-02-12 - club - PSL - male - 959199 - Islamabad United vs Peshawar Zalmi
2016-02-12 - club - PSL - male - 959197 - Karachi Kings vs Lahore Qalandars
2016-02-11 - club - PSL - male - 959195 - Islamabad United vs Quetta Gladiators
2016-02-10 - club - PSL - male - 959191 - Lahore Qalandars vs Islamabad United
2016-02-08 - club - PSL - male - 959189 - Lahore Qalandars vs Quetta Gladiators
2016-02-07 - club - PSL - male - 959187 - Peshawar Zalmi vs Quetta Gladiators
2016-02-07 - club - PSL - male - 959185 - Islamabad United vs Karachi Kings
2016-02-06 - club - PSL - male - 959183 - Lahore Qalandars vs Peshawar Zalmi
2016-02-06 - club - PSL - male - 959181 - Karachi Kings vs Quetta Gladiators
2016-02-05 - club - PSL - male - 959179 - Peshawar Zalmi vs Islamabad United
2016-02-05 - club - PSL - male - 959177 - Lahore Qalandars vs Karachi Kings
2016-02-04 - club - PSL - male - 959175 - Islamabad United vs Quetta Gladiators

Further information
-------------------

You can find all of our currently available data at https://cricsheet.org/

You can contact me via the following methods:
  Email   : stephen@cricsheet.org
  Mastodon: @cricsheet@deeden.co.uk
