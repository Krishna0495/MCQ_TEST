# MCQ Test Application
You have an app where teachers can create MCQ-based question papers. They can roll it out to the students who are enrolled for that test.
Points to ponder:
1) Each test has a fixed deadline and can vary from test to test.
2) The student has to complete it within that period; otherwise, they won't be able to
submit the test.
3) This test will have multiple pages (one MCQ per page). Only once you submit the
current question will you be able to move on to the next question. But you can move from any question to an already submitted question and resubmit the answer or just view it.
4) Once all questions are answered, you can submit the test.
5) This test can be attended in multiple sessions (each has a TTL of 30 minutes). When
you re-login to test, you will be redirected to the last page you were on in the previous session.
Some error cases to consider:
1) While submitting a question or test, the store call fails due to some system issue.
2) There is a bug in your app due to which students can submit the answers to any
question or submit the test even 15 minutes after the session has expired.

What you will do:
1) Create an end-to-end data pipeline solution including data models for source data for the above problem statement. Also, suggest/create a dashboard where you can answer the below questions:
   a. How many tests were started?
   
   b. How many tests were completed?
   
   c. How many tests were not completed and reasons for it.
   
   d. Timeline for each action (question) taken by the student. This includes time taken to solve the current question, time taken to reach this question from the last question          and time from the start of the test.
   
   e. A funnel view to show the test performance (not student performance) set by the teacher.
   
   f. Sessions level stats.
   
   g. Question level stats (most time-consuming question, mostly answered correctly
      questions, mostly revisited question, mostly answered wrong question etc).
   
3) You can add any metrics that you can think of here.
4) Define the refresh frequency of these metrics and their reasoning
5) Your solution should be able to scale for 1Cr tests attempted per day
