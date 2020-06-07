select
      c.name as course_name,
      u.name as university_name,
      su.PlanType as plan_type,
      s.StudentClient,
      s.city,
      s.state,
      sub.name as subject_name,
      count(*) as total_sessions,
      count(distinct s.id) as total_students
  from `l2_students.*` s
  left join `l2_courses.*` c on c.id = s.CourseId
  left join `l2_universities.*` u on u.id = s.UniversityId
  left join `l2_subscriptions.*` su on su.StudentId = s.id
  left join `l2_sessions.*` se on se.StudentId = s.id
  left join `l2_student_follow_subject.*` sfs on sfs.StudentId = s.Id
  left join `l2_subjects.*` sub on sub.id = sfs.SubjectId
group by 1, 2, 3, 4, 5, 6, 7
