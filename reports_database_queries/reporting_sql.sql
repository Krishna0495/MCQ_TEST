--1. How many tests were started?
-- Card Metrics 
-- Refresh rate: Daily refresh
select 
count(distinct(test_id))
from
session_log


--2. How many tests were completed?
-- Card Metrics 
-- Refresh rate: Daily refresh
select
count(distinct(test_id))
from
test_assignment
where submission_date is not null


--3. How many tests were not completed and reasons for it.
-- 3 Card Metrics 
    -- total_time_taken_in_hrs>deadline --time out
    -- not attempted 
    -- unanswered_questions
-- Refresh rate: Daily refresh
with time_taken as (
    select
    test_id,
    student_id,
    min(session_start_time) as test_start_time,
    max(session_end_time) as test_end_time
    sum(datediff('h',session_start_time,session_end_time)) as total_time_taken_in_hrs
    from
    session_log
    group by
    test_id,
    student_id

)
,
unanswered_questions as (
    select
    sl.student_id,
    q.test_id,
    case when sl.question_id is null then q.question_id else null end as unanswered_questions,
    from
    question q 
    left join
    session_log sl 
    on sl.question_id=q.question_id
    and sl.test_id=q.test_id
    where concat(sl.student_id,sl.test_id,sl.question_id) not in (
        select concat(a.student_id,a.test_id,a.question_id)
        from
        answer a
    )
    group by
    sl.student_id,
    sl.question_id,
    q.test_id

)

-- )

select
count(distinct(case when tt.total_time_taken_in_hrs > t.deadline then tt.test_id else null end)) as number_of_overtime_test
count(distinct(case when tt.student_id is null then ta.test_id else null end)) as number_of_unattempted_test,
count(distinct(unanswered_questions)) as unanswered_questions_count
from
test t 
inner join
test_assignment ta
on t.test_id=ta.test_id
left join
time_taken tt 
on tt.test_id=ta.test_id
and tt.student_id=ta.student_id
left join
unanswered_questions uq 
on uq.test_id=ta.test_id
and uq.student_id=ta.student_id
where ta.submission_date is null
;

-- 2) There is a bug in your app due to which students can submit the answers to any
-- question or submit the test even 15 minutes after the session has expired.

-- tabular view with test and student details to debug error
-- Refresh rate: Daily refresh

with time_taken as (
    select
    test_id,
    student_id,
    question_id
    min(session_start_time) as test_start_time,
    max(session_end_time) as test_end_time
    sum(datediff('h',test_start_time,test_end_time)) as total_time_taken_in_hrs
    from
    session_log
    group by
    test_id,
    student_id,
    question_id

)

select
sum(total_time_taken_in_hrs) over(partition by student_id,test_id order by question_id) as total_test_time_in_hrs,
case when tt.total_test_time_in_hrs > t.deadline  then tt.test_id else null end as test_id_of_invalid_test_app_error,
ta.student_id,
ta.submission_date
from
test t 
inner join
test_assignment ta
on t.test_id=ta.test_id
left join
time_taken tt 
on tt.test_id=ta.test_id
and tt.student_id=ta.student_id
where ta.submission_date is not null

-- Timeline for each action (question) taken by the student. This includes time taken to solve the current question, time taken to reach this question from the last question and time from the start of the test.
-- test id , student id, question id, time_taken_question_id , time_taken_test_id , lag time_taken_question_id

-- tabular view with test
-- Refresh rate: Daily refresh

select
test_id,
student_id,
question_id,
time_taken_question_id_hrs,
time_taken_question_id_solved_hrs,
time_taken_reach_next_question_id_hrs,
sum(time_taken_question_id_hrs) over(partition by test_id,student_id order by question_id) as time_taken_test_id_hrs,
sum(time_taken_question_id_hrs) over(partition by test_id,student_id order by question_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_time_taken_in_test
from
(
select
test_id,
student_id,
question_id,
datediff('h',start_time_question_id,end_time_question_id) as time_taken_question_id_hrs,
datediff('h',start_time_question_id,end_time_question_id_solved) as time_taken_question_id_solved_hrs,
lag(end_time_question_id,1) over(partition by test_id,student_id,question_id order by end_time_question_id asc) as prev_question_end_time,
datediff('h',prev_question_end_time,start_time_question_id) as time_taken_reach_next_question_id_hrs,
from
(
select
test_id,
student_id,
question_id,
min(session_start_time) as start_time_question_id
max(session_end_time) as end_time_question_id
max(answer_timestamp) as end_time_question_id_solved
from
session_log  sl
join
answer a 
on sl.test_id=a.test_id
and sl.student_id = a.student_id
and sl.question_id=a.question_id
group by
test_id,
student_id,
question_id
)
)

-- A funnel view to show the test performance (not student performance) set by the teacher.

-- funnel view 
-- Refresh rate: Daily refresh

with time_taken as (
    select
    test_id,
    student_id,
    sum(datediff('h',session_start_time,session_end_time)) as total_time_taken_in_hrs
    from
    session_log
    group by
    test_id,
    student_id

)

select
t.test_id,
case when status=1 then t.test_id else null end as active_test_id,
case when status=0 then t.test_id else null end as inactive_test_id,
t.total_questions,
avg(t.deadline) as max_time_test,
count(distinct(tas.student_id)) as total_student_enrolled,
count(distinct(case when tas.submission_date is not null then tas.test_id else null end)) as total_submitted_test,
count(distinct(case when tas.submission_date is null then tas.test_id else null end)) as total_unsubmitted_test,
count(distinct(case when tt.total_time_taken_in_hrs > t.deadline then tt.test_id else null end)) as test_failed_number_of_overtime_test,
count(distinct(case when tt.student_id is null then tas.test_id else null end)) as number_of_unattempted_test,
sum(tt.total_time_taken_in_hrs)/count(distinct(tt.student_id)) as avg_time_taken_per_student

from
test t
left join
test_assignment tas
on t.test_id=tas.test_id
left join
time_taken tt 
on tt.test_id=tas.test_id
and tt.student_id=tas.student_id


-- Sessions level stats.
-- line chart view 
-- Refresh rate: 1-2 hours refresh

with session_stats as (
select
test_id,
student_id,
question_id,
-- sum(datediff('h',session_start_time,session_end_time)) as total_time_taken_in_hrs,
-- avg(datediff('h',session_start_time,session_end_time)) as avg_taken_in_hrs,
count(distinct session_id) as total_session_id_each_test_per_student,
min(session_start_time) as test_start_datetime,
max(session_end_time) as test_end_datetime,
-- min(Extract(hour from session_start_time)) as start_hour
-- max(Extract(hour from session_end_time)) as end_hour
from
session_log
group by
test_id,
student_id,
question_id
)

select
cast(test_start_datetime as date) as session_start_date,
cast(test_end_datetime as date) as session_end_date,
count(distinct test_id) as no_of_tests_per_day,
count(distinct student_id) as no_of_students_per_day,
sum(total_session_id_each_test_per_student) as total_sessions,
count(distinct question_id) as no_of_questions_per_day,
from
session_stats
group by
cast(test_start_datetime as date),
cast(test_end_datetime as date)
;

with session_stats as (
select
test_id,
student_id,
question_id,
-- sum(datediff('h',session_start_time,session_end_time)) as total_time_taken_in_hrs,
-- avg(datediff('h',session_start_time,session_end_time)) as avg_taken_in_hrs,
count(distinct session_id) as total_session_id_each_test_per_student,
min(session_start_time) as test_start_datetime,
max(session_end_time) as test_end_datetime
from
session_log
group by
test_id,
student_id,
question_id
)

select
Extract(hour from test_start_datetime)  as session_start_hour,
count(distinct test_id) as no_of_tests_per_day,
count(distinct student_id) as no_of_students_per_day,
sum(total_session_id_each_test_per_student) as total_sessions,
count(distinct question_id) as no_of_questions_per_day,
from
session_stats
group by
Extract(hour from test_start_datetime)

-- Question level stats (most time-consuming question, mostly answered correctly
-- questions, mostly revisited question, mostly answered wrong question etc).

-- Sessions level stats.
-- line chart view 
-- Refresh rate: Daily refresh

with time_taken as (
    select
    test_id,
    question_id,
    student_id,
    min(session_start_time) as question_start_time,
    max(session_end_time) as question_end_time
    from
    session_log
    group by
    test_id,
    question_id,
    student_id

),
correct_answer as (
    select
    q.question_id,
    c.choice_id as correct_choice_id
    from
    question q
    join
    choice c 
    on q.question_id=c.question_id
    where c.is_correct=1
),
time_to_answer as (
    select
    sl.question_id,
    sl.student_id,
    a.choice_id,
    min(sl.session_start_time) as question_start_time,
    max(a.answer_timestamp) as answered_timestamp,
    from
    session_log sl
    join
    answer a 
    on sl.question_id=a.question_id
    and sl.student_id=a.student_id
    and sl.test_id=sl.test_id

),
deduplicate_session_logs as (

    select
    sl.question_id,
    sl.student_id,
    sl.session_start_time
    from
    session_log sl
    group by
    sl.question_id,
    sl.student_id,
    sl.session_start_time

),
ranked_logs as (
    select
    student_id,
    question_id,
    session_start_time,
    lag(question_id) over(partition by student_id order by session_start_time) as prev_question_id,
    ROW_NUMBER() over(partition by student_id order by session_start_time) as row_num
    from
    deduplicate_session_logs

),
question_change as (
    select
    student_id,
    question_id,
    session_start_time,
    prev_question_id,
    case when question_id!=prev_question_id then 'Question visited' else 'No changed' end as change_status
    from
    ranked_logs
    order by 
    student_id,
    session_start_time
),
revisited_questions as (
    select
    question_id,
    sum(case when change_status = 'Question visited' then 1 else 0 end) sum_change_status
    from
    question_change
    group by
    question_id
)

select
a.question_id,
avg(datediff('h',tt.question_start_time,tt.question_end_time)) avg_time_spent_question,
max(datediff('h',tt.question_start_time,tt.question_end_time)) max_time_spent_question,
avg(datediff('h',tt.question_start_time,tta.answered_timestamp)) avg_time_to_answer,
max(datediff('h',tt.question_start_time,tta.answered_timestamp)) max_time_to_answer,
case when a.choice_id=ca.correct_choice_id then 1 else 0 end no_correct_ans,
case when a.choice_id!=ca.correct_choice_id then 1 else 0 end no_incorrect_ans,

avg(
    case when tta.choice_id=ca.correct_choice_id
    then datediff('h',tt.question_start_time, tta.answer_timestamp)
    else NULL end
    ) as avg_time_correct_ans,

max(
    case when tta.choice_id=ca.correct_choice_id
    then datediff('h',tt.question_start_time, tta.answer_timestamp)
    else NULL end
    ) as max_time_correct_ans,

sum(sum_change_status) as number_of_times_visited

from
question q 
left join
answer a 
on q.question_id=a.question_id
left join
time_taken tt 
on tt.question_id = a.question_id
and tt.student_id=a.student_id
left join 
correct_answer ca
on ca.question_id = a.question_id
left join
time_to_answer tta 
on ca.question_id = a.question_id
and tt.student_id=a.student_id
left join
revisited_questions rq 
on rq.question_id = q.question_id

-- Aggregated Tables

-- Test_Stats
select
t.test_id,
t.total_questions,
case when status=1 then t.test_id else null as active_test,
case when status=0 then t.test_id else null as inactive_test,
sl.student_id,
sl.question_id,
a.choice_id,

case when c.is_correct=1 then a.choice_id else null end as correct_answer,

DATEDIFF('h',sl.session_start_time,sl.session_end_date) as total_time_spend_per_student_per_question,

case when sl.session_end_date=a.answer_timestamp then 
DATEDIFF('h',sl.session_start_time,a.answer_timestamp) else null 
end as total_time_spend_per_student_per_question_to_answer

from
test t 
left join
test_assignment ta
on ta.test_id=t.test_id
left join
session_log sl
on sl.student_id=ta.student_id
and sl.test_id=ta.test_id
left join
answer a 
on sl.student_id=a.student_id
and sl.test_id=a.test_id
and sl.question_id=a.question_id
left join
choice c
and a.question_id=c.question_id
and a.choice_id=c.choice_id
;

