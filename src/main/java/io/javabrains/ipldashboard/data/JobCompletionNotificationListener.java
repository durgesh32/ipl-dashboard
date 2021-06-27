package io.javabrains.ipldashboard.data;

import io.javabrains.ipldashboard.models.Team;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import java.util.HashMap;
import java.util.Map;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final EntityManager entityManager;

    @Autowired
    public JobCompletionNotificationListener(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    @Transactional
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            System.out.println("!!! JOB FINISHED! Time to verify the results");
            Map<String, Team> teamMap = new HashMap<>();
            var query = "select m.team1, count(m.team1) from Match m group by m.team1";
            entityManager.createQuery(query, Object[].class)
                    .getResultList().stream().map(e -> new Team((String) e[0], (long) e[1])).forEach(e -> teamMap.put(e.getTeamName(), e));

            query = "select m.team2, count(m.team2) from Match m group by m.team2";
            entityManager.createQuery(query, Object[].class)
                    .getResultList().forEach(e -> {
                var team = teamMap.get(e[0]);
                team.setTotalMatches(team.getTotalMatches() + (long) e[1]);
            });

            query = "select m.matchWinner, count(m.matchWinner) from Match m group by m.matchWinner";
            entityManager.createQuery(query, Object[].class)
                    .getResultList().forEach(e -> {
                var team = teamMap.get(e[0]);
                if (team != null) team.setTotalWins((long) e[1]);
            });
            teamMap.values().forEach(entityManager::persist);
            teamMap.values().forEach(System.out::println);
        }
    }
}