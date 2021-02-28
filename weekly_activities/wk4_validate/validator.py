"""
created by Genevieve LaLonde
Data Engineering CS510
Weeks 4, 5: Data Validation
"""

import pandas as pd
df = pd.read_csv("crash_data.csv")

CrashesDF = df[df['Record Type'] == 1]
VehiclesDF = df[df['Record Type'] == 2]
ParticipantsDF = df[df['Record Type'] == 3]

# Drop axis 1: drop columns which are missing values.
# All: drop the whole column.

CrashesDF = CrashesDF.dropna(axis=1,how='all')
VehiclesDF = VehiclesDF.dropna(axis=1,how='all')
ParticipantsDF = ParticipantsDF.dropna(axis=1,how='all')

###########################################################################
# Existence Assertions
###########################################################################

# Every crash has a vehicle, bicycle, or motorcycle associated.
print('Starting test: Existence a: every crash has an associated vehicle.')

"""
This assertion may need reassessment. 
Most crashes have 2 vehicle IDs associated. 
"""
association_count = 0
for crash in CrashesDF['Crash ID']:
	# Reset on each crash, so if any crash fails, fail this test.
	association = False
	for vehicle in VehiclesDF['Crash ID']:
		if vehicle == crash:
			#print('crash: ', str(crash))
			#print('vehicle: ' + str(vehicle))
			association = True
			association_count += 1
			break;
	if association == False:
		print('Fail: Crash has no vehicle: ' + crash)
		break;
if (association_count != CrashesDF['Crash ID'].size): 
	print('Fail: at least one crash failed association test. \
		Crash count = ' + str(CrashesDF['Crash ID'].size) + \
		' and association count = ' + str(association_count))
else:
	print('Pass: there are ' + str(CrashesDF['Crash ID'].size) + \
		' crashes and ' + str(association_count) + \
		' of them have at least one associated vehicle.'
		)

# Each crash has a Serial Number.
print('Starting test: Existence b: every crash has serial number.')
serials_exist = True
if CrashesDF['Serial #'].dropna().size != CrashesDF['Serial #'].size:
	serials_exist = False
if serials_exist:
	print('Pass: all crashes have serial numbers.')
else:
	print('Fail: there are ' + CrashesDF['Serial #'].size + \
		' crashes but only ' + CrashesDF['Serial #'].dropna().size + \
		' of them have serial numbers.')

###########################################################################
# Limit Assertions
###########################################################################

# There are 3 record types, each record must belong to one of the types. 
# Record type is in set(1,2,3)

print('Starting test: Limit a: records are 1, 2, or 3.')

"""
As a more complex test, this confirms:  
 * crashes are type 1
 * vehicles are type 2
 * participants are type 3
"""

record_set_breached = False
crash_one = True
vehicle_two = True
participant_three = True
for record in CrashesDF['Record Type']:
	if record != 1:
		crash_one = False
for record in VehiclesDF['Record Type']:
	if record != 2:
		vehicle_two = False
for record in ParticipantsDF['Record Type']:
	if record != 3:
		participant_three = False
if crash_one & vehicle_two & participant_three:
	print('Pass: crashes have record type 1, ' + \
		'vehicles have record type 2, ' + \
		'and participants have record type 3.')
else:
	print('Fail: record set breach.')

# The speed limit is positive and less than 101.
# 101 > POST_SPEED_LMT_VAL > 0

# TODO reassess. This is a T/F field, not an actual speed.
# Side note, it is passing, but only cause 0 and 1 fall in the range.
print('Starting test: limit b: speed limit between 101 and 0.')

if (CrashesDF['Posted Speed Limit'].max() <= 101) & (CrashesDF['Posted Speed Limit'].min() >= 0):
	print('Pass: speed limit is between 0 and 101.')
else:
	print('Fail: Speed limit wrong.')

###########################################################################
# Intra-record Assertions
###########################################################################

# Total pedestrians involved is greater than the count of pedestrial fatalities or injuries. 
# This does not pass evaluation.

print('Starting test: Intra-record a: pedestrian total > pedestrian damaged.')

damaged_pedestrian_spawning = False
for crash in CrashesDF.to_numpy():
	if crash[83] < crash[84] + crash[85]:
		damaged_pedestrian_spawning = True
		print('pedestrians: ' + str(crash[83]))
		print('Pedestrian fatalities: ' + str(crash[84]))
		print('Pedestrians injured non-fatally: ' + str(crash[85]))
		break
if damaged_pedestrian_spawning:
	print('Fail: damaged pedestrians are spawning.')
else:
	print('Pass: there exists at least as many pedestrians as hurt pedestrians.')

		
# If the Speed Involved Flag is not null, there is a greater likelihood of fatal injuries TOT_FATAL_CNT. Aka 
sum(CRASH_SPEED_INVLV_FLG)/ sum(TOT_FATAL_CNT) > 0
crashes Speed Involved Flag

vehicles Vehicle Exceeded Posted Speed Flag

avg fatality group by speed Flag
crashes Total Fatality Count

join(other.set_index('key'), on='key')


print('Starting test: Intra record b:')
CrashesDF.groupby('Speed Involved Flag').mean()
VehiclesDF.groupby('Vehicle Exceeded Posted Speed Flag').mean()


if True:
	print('Pass: .')
else:
	print('Fail: .')


###########################################################################
# Inter-record Assertions
###########################################################################

# The average count of vehicles involved is between 0 and 3.
# 3 >= avg(TOT_VHCL_CNT) >= 0
"""
print('Starting test: ')

if True:
	print('Pass: .')
else:
	print('Fail: .')
"""
# There are crashes every month of the year.

###########################################################################
# Summary Assertions
###########################################################################

# Every record has a crash ID.

# Crash IDs are unique

print('Starting test: Summary a: every crash ID is unique.')

crashes_are_unique = True
unique_crashes = set()
for crash in CrashesDF['Crash ID']:
	unique_crashes.add(crash)
if len(unique_crashes) != CrashesDF['Crash ID'].size:
	print('There are fewer unique crashes \
		(' + str(len(unique_crashes)) + ') \
		than the total count of crashes \
		(' + str(CrashesDF['Crash ID'].size) + ')')
	crashes_are_unique = False
if crashes_are_unique:
	print('Pass: Crashes are unique.')
else:
	print('Fail: Crashes are not unique.')

###########################################################################
# Referential Assertions
###########################################################################

# Each participant has a crash ID and vehicle ID (even if it is 0)

# Each vehicle has a crash ID

###########################################################################
# Statistical Assertions
###########################################################################

# Fatalities as a percentage of crashes is even throughout the year.

# Passengers in vehicle has a normal form distribution






