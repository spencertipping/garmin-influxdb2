import garminconnect as gc
import influxdb_client as ic

import argparse as ap
import datetime as dt

import json
import getpass
import sys


def day_hrv(d, e, g, w):
  hrv = g.get_hrv_data(d)

  if hrv:
    w(ic.Point('hrv')
      .field('weekly_avg', hrv['hrvSummary']['weeklyAvg'])
      .field('night_avg',  hrv['hrvSummary']['lastNightAvg'])
      .time(hrv['hrvSummary']['createTimeStamp']))

    if hrv['hrvSummary'].get('baseline') is not None:
      w(ic.Point('hrv')
        .field('baseline_low_upper', hrv['hrvSummary']['baseline']['lowUpper'])
        .field('baseline_low_balanced', hrv['hrvSummary']['baseline']['balancedLow'])
        .field('baseline_upper_balanced', hrv['hrvSummary']['baseline']['balancedUpper'])
        .field('marker_value', hrv['hrvSummary']['baseline']['markerValue'])
        .time(hrv['hrvSummary']['createTimeStamp']))

    w([ic.Point('hrv').field('hrv', x['hrvValue']).time(x['readingTimeGMT'])
       for x in hrv['hrvReadings']])


def day_hr(d, e, g, w):
  hr = g.get_heart_rates(d)

  if hr['heartRateValues']:
    w([ic.Point('heart_rate').field('bpm', v).time(ms, ic.WritePrecision.MS)
       for ms, v in hr['heartRateValues']])

  w(ic.Point('heart_rate')
    .field('max', hr['maxHeartRate'])
    .field('min', hr['minHeartRate'])
    .field('resting', hr['restingHeartRate'])
    .time(int(e + 43200), ic.WritePrecision.S))


def day_steps(d, e, g, w):
  w([ic.Point('steps')
     .field('count', s['steps'])
     .tag('activity_level', s['primaryActivityLevel'])
     .tag('activity_level_constant', s['activityLevelConstant'])
     .time(s['endGMT'] + 'Z') for s in g.get_steps_data(d)])


def day_stress(d, e, g, w):
  s = g.get_stress_data(d)
  w([ic.Point('stress')
     .field('value', v)
     .time(ms, ic.WritePrecision.MS) for ms, v in s['stressValuesArray']])

  if 'bodyBatteryValuesArray' in s:
    w([ic.Point('body_battery')
       .field('value', v)
       .time(ms, ic.WritePrecision.MS) for ms, _, v, _ in s['bodyBatteryValuesArray']])

  w(ic.Point('stress')
    .field('max', s['maxStressLevel'])
    .field('avg', s['avgStressLevel'])
    .time(int(e + 43200), ic.WritePrecision.S))


def day_sleep(d, e, g, w):
  s = g.get_sleep_data(d)
  d = s['dailySleepDTO']

  w([ic.Point('sleep')
     .field('activity_level', m['activityLevel'])
     .time(m['endGMT'] + 'Z') for m in s['sleepMovement']])

  if 'sleepScores' in s:
    w(ic.Point('sleep')
       .tag('sleep_score_feedback', s['sleepScoreFeedback'])
       .tag('sleep_score_insight', s['sleepScoreInsight'])
       .field('sleep_score', s['sleepScores']['overall']['value'])
       .field('rem_percentage', s['sleepScores']['remPercentage']['value'])
       .field('deep_percentage', s['sleepScores']['deepPercentage']['value'])
       .field('light_percentage', s['sleepScores']['lightPercentage']['value'])
       .time(d['sleepEndTimestampGMT'], ic.WritePrecision.MS))

  if 'sleepStress' in s:
    w([ic.Point('sleep')
       .field('stress', x['value'])
       .time(x['startGMT'], ic.WritePrecision.MS) for x in s['sleepStress']])

  if 'wellnessEpochRespirationDataDTOList' in s:
    w([ic.Point('sleep')
       .field('respiration', x['respirationValue'])
       .time(x['startTimeGMT'], ic.WritePrecision.MS)
       for x in s['wellnessEpochRespirationDataDTOList']])

  w(ic.Point('sleep')
    .tag('sleep_window_confirmed', d['sleepWindowConfirmed'])
    .tag('sleep_window_confirmation_type', d['sleepWindowConfirmationType'])
    .field('sleep_seconds', d['sleepTimeSeconds'])
    .field('nap_seconds', d['napTimeSeconds'])
    .field('mean_sleep_stress', d.get('avgSleepStress'))
    .field('awake_count', d.get('awakeCount'))
    .field('mean_respiration', d.get('averageRespirationValue'))
    .field('min_respiration', d.get('minRespirationValue'))
    .field('max_respiration', d.get('maxRespirationValue'))
    .field('restless_moments', d.get('restlessMomentsCount'))
    .field('unmeasurable_seconds', d['unmeasurableSleepSeconds'])
    .field('deep_seconds', d['deepSleepSeconds'])
    .field('light_seconds', d['lightSleepSeconds'])
    .field('rem_seconds', d['remSleepSeconds'])
    .field('awake_seconds', d['awakeSleepSeconds'])
    .time(d['sleepEndTimestampGMT'], ic.WritePrecision.MS))


def day_user(d, e, g, w):
  u = g.get_user_summary(d)
  w(ic.Point('user')
    .field('total_kcal', u['totalKilocalories'])
    .field('active_kcal', u['activeKilocalories'])
    .field('bmr_kcal', u['bmrKilocalories'])
    .field('wellness_kcal', u['wellnessKilocalories'])
    .field('total_steps', u['totalSteps'])
    .field('total_meters', u['totalDistanceMeters'])
    .field('step_goal', u['dailyStepGoal'])
    .field('highly_active_seconds', u['highlyActiveSeconds'])
    .field('active_seconds', u['activeSeconds'])
    .field('sedenary_seconds', u['sedentarySeconds'])
    .field('sleeping_seconds', u['sleepingSeconds'])
    .field('moderate_intensity_minutes', u['moderateIntensityMinutes'])
    .field('vigorous_intensity_minutes', u['vigorousIntensityMinutes'])
    .field('floors_ascended_meters', u['floorsAscendedInMeters'])
    .field('floors_descended_meters', u['floorsDescendedInMeters'])
    .field('min_heart_rate', u['minHeartRate'])
    .field('max_heart_rate', u['maxHeartRate'])
    .field('resting_heart_rate', u['restingHeartRate'])
    .field('average_stress', u['averageStressLevel'])
    .field('max_stress', u['maxStressLevel'])
    .field('stress_seconds', u['stressDuration'])
    .field('rest_stress_seconds', u['restStressDuration'])
    .field('activity_stress_seconds', u['activityStressDuration'])
    .field('uncategorized_stress_seconds', u['uncategorizedStressDuration'])
    .field('low_stress_seconds', u['lowStressDuration'])
    .field('medium_stress_seconds', u['mediumStressDuration'])
    .field('high_stress_seconds', u['highStressDuration'])
    .field('measurable_awake_seconds', u['measurableAwakeDuration'])
    .field('measurable_sleep_seconds', u['measurableAsleepDuration'])
    .field('body_battery_charged', u['bodyBatteryChargedValue'])
    .field('body_battery_drained', u['bodyBatteryDrainedValue'])
    .field('body_battery_max', u['bodyBatteryHighestValue'])
    .field('body_battery_min', u['bodyBatteryLowestValue'])
    .field('resting_kcal_from_activity', u['restingCaloriesFromActivity'])
    .time(int(e + 43200), ic.WritePrecision.S))


def day(e, g, w):
  d = dt.datetime.fromtimestamp(se).strftime('%Y-%m-%d')
  print(f'uploading {d}...', end='', flush=True)
  day_hrv   (d, e, g, w); print(' [hrv]',    end='', flush=True)
  day_hr    (d, e, g, w); print(' [hr]',     end='', flush=True)
  day_steps (d, e, g, w); print(' [steps]',  end='', flush=True)
  day_stress(d, e, g, w); print(' [stress]', end='', flush=True)
  day_sleep (d, e, g, w); print(' [sleep]',  end='', flush=True)
  day_user  (d, e, g, w); print(' [user]',   end='', flush=True)
  print()


p = ap.ArgumentParser(description='Import Garmin Connect data into InfluxDB2')
p.add_argument('--start', help='start date in YYYY-mm-dd format')
p.add_argument('--end',   help='end date in YYYY-mm-dd format')
p.add_argument('--email', help='Garmin Connect email address')

p.add_argument('--server', help='influxDB server URL, e.g. http://myinfluxdb:8086')
p.add_argument('--org',    help='influxDB org')
p.add_argument('--bucket', help='influxDB bucket')
p.add_argument('--token',  help='influxDB auth token, a big base64 string')

a = p.parse_args()


g = gc.Garmin(a.email, getpass.getpass())
assert g.login(), "garmin login failed"


with ic.InfluxDBClient(url=a.server, org=a.org, token=a.token) as i:
  w  = i.write_api(write_options=ic.client.write_api.SYNCHRONOUS)
  se = dt.datetime.strptime(a.start, '%Y-%m-%d').timestamp()
  ee = dt.datetime.strptime(a.end,   '%Y-%m-%d').timestamp()

  while se <= ee:
    day(se, g, lambda p: w.write(a.bucket, a.org, p))
    se += 86400    # seconds per day
