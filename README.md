# classroom-music
Raspberry Pi scheduler to set musical queues based on bell schedule and teacher schedule.

# Approach

Suggested by [J. Bowman](https://gist.github.com/gitblight1) in a [gist](https://gist.github.com/gitblight1/602f0a73672822c1ef6b056ff35ea293), use a program called [vlc](https://www.videolan.org/vlc/), which comes with a command line interface `cvlc` that is [documented here](https://wiki.videolan.org/Documentation:Streaming_HowTo/Command_Line_Examples/) and also maybe [here](https://openbase.com/js/cvlc/documentation).

The key command is:

```
cvlc --play-and-exit fname
```

And even a bit further, this can be included in the cronfile by adding lines using `sudo crontab -e`.

But for my purposes, we need a python program to schedule multiple short musical events or interludes rather than just playing things at random.

## The basics

There is a bell schedule which defines when classes begin and end.
The bell schedule may change on the fly and it may be pretty complicated.

There is a teacher's schedule which determines which classes the teacher is actually teaching.

Then there is the musical schedule for each class, eg: t=0, play a hello tune, t=5min, play a now we begin tune, t=END-5min play a checkout tune.
And there needs to be a way to switch schedules when there is a delayed opening or some other event.

All these files can be managed by hand, all are `.csv`.
### bell schedule `bells.csv`

`schedule name, period, start-time, end-time`

### teacher schedule `teachers.csv`

`teacher, day-of-week, period begin, period end, room, class name`


### class schedule `class.csv`

`class name, lesson category, bell name, dt, after-start=0/before-end=1`

### music selection `music.csv`

`class name, lesson category, bell name, music fname`

### calendar `calendar.csv`

`date, bell schedule`

## usage

```
$ python -v set_bells [--date date] [--schedule schedule_name]
Setting bells for today. Which schedule:
1) regular
2) delayed opening
3) late opening
4) development day
[enter choice :] 1
Bells set
... [list of musical interludes scheduled]
$
```


## implementation

1. use packages `crontab`, `csv`, and `sqlite3` (for all the joins to get specific times from the list of schedules).
1. add a real time clock to the Pi so that it can be scooped away, set, returned. And so that it can work without recourse to the internet.
1. find some decent music.


