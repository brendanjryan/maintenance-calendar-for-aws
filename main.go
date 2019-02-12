package main

import (
	"fmt"
	"time"
	"strconv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/health"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/rmkbow/ical-go"
	"net/url"
	"os"
	"strings"
	"flag"
	"log"
)

var awsSession *session.Session
var healthConnection *health.Health
var ec2Connection map[string]*ec2.EC2
var rdsConnection map[string]*rds.RDS
var elasticacheConnection map[string]*elasticache.ElastiCache

func errorCheck(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	filename := flag.String("filename", "", "Filename of the local and destination file")
  region := flag.String("region", "us-east-1", "Region commands should be run in")

	flag.Parse()
	exFlag := 0

	if *filename == "" {
		exFlag = 2
		fmt.Println("--filename FILENAME")
	}

  if *region == "" {
    exFlag = 2
    fmt.Println("--filename REGION")
  }

	if exFlag != 0 {
		os.Exit(exFlag)
	}
	log.Println("initializing...")
	initialize(*region)
	log.Println("successfully initialized!")
	calendar := calendar(calendar_events(healthEvents()))
	log.Println("saving calendar to file...!")
	save_calendar_to_file(*filename, calendar)
	log.Println("successfully saved calendar to file!")
}

func initialize(region string) {
	awsSession, _ = session.NewSession()
	ec2Connection = make(map[string]*ec2.EC2)
	healthConnection = health.New(awsSession, &aws.Config{Region: aws.String(region)})
	rdsConnection = make(map[string]*rds.RDS)
	elasticacheConnection = make(map[string]*elasticache.ElastiCache)
}

func initializeEC2Connection(region string) {
	ec2Connection[region] = ec2.New(awsSession, &aws.Config{Region: aws.String(region)})
}

func initializeRDSConnection(region string) {
	rdsConnection[region] = rds.New(awsSession, &aws.Config{Region: aws.String(region)})
}

func initializeElasticacheConnection(region string) {
	elasticacheConnection[region] = elasticache.New(awsSession, &aws.Config{Region: aws.String(region)})
}

func healthEvents() []*health.Event {
	log.Println("getting health events...")
	describeEventFilter := &health.EventFilter{
		EventTypeCategories: []*string{aws.String("scheduledChange")},
		EventStatusCodes: []*string{aws.String("open"), aws.String("upcoming")},
	}
	describeEventParams := &health.DescribeEventsInput{
		Filter: describeEventFilter,
	}
	describeEventParams.SetMaxResults(100)
	healthEvents, err := healthConnection.DescribeEvents(describeEventParams)
	if err != nil {
		log.Fatal("error getting health events: ", err)
	}

	return healthEvents.Events
}

func resource_ids(health_arn *string) []string {
	describeAffectedEntitiesParams := &health.DescribeAffectedEntitiesInput{
		Filter: &health.EntityFilter{
			EventArns: []*string{health_arn},
		},
	}
	describeAffectedEntitiesParams.SetMaxResults(100)
	var resourceIds []string
	affectedEntities, _ := healthConnection.DescribeAffectedEntities(describeAffectedEntitiesParams)
	for _, entity := range affectedEntities.Entities {
		resourceIds = append(resourceIds,*entity.EntityValue)
	}
	return resourceIds
}

func process_event(health_arn *string) ([]string, string, string, string, *time.Time, *time.Time) {

	resourceIds := resource_ids(health_arn)

	describeEventParams := &health.DescribeEventDetailsInput{
		EventArns: []*string{health_arn},
	}
	detailedEvents, err := healthConnection.DescribeEventDetails(describeEventParams)
	if err != nil {
		log.Fatal("error describing event details: ", err)
	}
	var description string
	var et string
	var es string
	var est *time.Time
	var eet *time.Time
	for _, set := range detailedEvents.SuccessfulSet {
		description = *set.EventDescription.LatestDescription
		et = *set.Event.EventTypeCode
		es = *set.Event.Service
		est = set.Event.StartTime
		eet = set.Event.EndTime
	}
	return resourceIds, description, et, es, est, eet
}

func ec2InstanceName(id string, region string) string {
	params := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("instance-id"),
				Values: []*string{
					aws.String(id),
				},
			},
		},
	}

	var ec2din_response *ec2.DescribeInstancesOutput
	var err error
	if ec2Connection[region] == nil {
		initializeEC2Connection(region)
	}
	ec2din_response, err = ec2Connection[region].DescribeInstances(params)
	errorCheck(err)

	var name string
	for _, reservations := range ec2din_response.Reservations {
		for _, instance := range reservations.Instances {
			for _, tag := range instance.Tags {
				if *tag.Key == "Name" {
					name = url.QueryEscape(*tag.Value)
				}
			}
		}
	}
	return name
}


func rds_maintenance_window(name string, region string) string {
	var rds_maintenance_window string

	if rdsConnection[region] == nil {
		initializeRDSConnection(region)
	}

	describe_rds_cluster_filter := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(name),
	}

	db_clusters, err := rdsConnection[region].DescribeDBClusters(describe_rds_cluster_filter)
	if err != nil {
		log.Fatal("error describing database clusters: ", err)
	}
	for _,db_cluster := range db_clusters.DBClusters {
		rds_maintenance_window = *db_cluster.PreferredMaintenanceWindow
	}

	if rds_maintenance_window != "" {
		return rds_maintenance_window
	}

	describe_rds_instance_filter := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(name),
	}
	db_instances, err := rdsConnection[region].DescribeDBInstances(describe_rds_instance_filter)
	if err != nil {
		log.Fatal("error describing database instance: ", err)
	}
	for _,db_instance := range db_instances.DBInstances {
		rds_maintenance_window = *db_instance.PreferredMaintenanceWindow
	}

	return rds_maintenance_window
}

func elasticache_maintenance_window(name string, region string) string {
	var elasticache_maintenance_window string

	if elasticacheConnection[region] == nil {
		initializeElasticacheConnection(region)
	}

        elasticache_name_normalized := strings.Replace(name, "/", "_", -1)
	elasticache_name_split_underscore := strings.Split(elasticache_name_normalized, "_")
	elasticache_name_underscore_trimmed := elasticache_name_split_underscore[:len(elasticache_name_split_underscore) - 2]
	elasticache_name := strings.Join(elasticache_name_underscore_trimmed,"_")


	elasticache_number_split_dash := strings.Split(name, "-")
	elasticache_number_dash_trimmed := elasticache_number_split_dash[len(elasticache_number_split_dash) -1]

	elasticache_number, _ := strconv.Atoi(elasticache_number_dash_trimmed)

	describe_elasticache_replica_filter := &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String(elasticache_name),
	}

	replica_sets,_ := elasticacheConnection[region].DescribeReplicationGroups(describe_elasticache_replica_filter)

	var elasticache_cluster_name string

	for _,replica_set := range replica_sets.ReplicationGroups {
		elasticache_cluster_name = *replica_set.MemberClusters[elasticache_number -1]
	}

	describe_elasticache_cluster_filter := &elasticache.DescribeCacheClustersInput{
		CacheClusterId: aws.String(elasticache_cluster_name),
	}

	clusters,_ := elasticacheConnection[region].DescribeCacheClusters(describe_elasticache_cluster_filter)


	for _,cluster := range clusters.CacheClusters {
		elasticache_maintenance_window = *cluster.PreferredMaintenanceWindow
	}
	return elasticache_maintenance_window
}


func calendar(calendar_events []ical.CalendarEvent) ical.Calendar {
	calendar := ical.Calendar{calendar_events}
	return calendar
}

func save_calendar_to_file(filename string, calendar ical.Calendar) {
	f, _ := os.Create(filename)
	defer f.Close()
	fmt.Fprintf(f, calendar.ToICS())
}

func calendar_event(id string, summary string, description string, location string, start_time *time.Time, end_time *time.Time) ical.CalendarEvent {
	calendar_event := ical.CalendarEvent{
		Id: id,
		Summary: summary,
		Description: description,
		Location: location,
		URL: "https://phd.aws.amazon.com/phd/home?region=us-east-1#/dashboard/scheduled-changes",
		StartAt: start_time,
		EndAt: end_time,
	}
	return calendar_event
}

func calendar_events(health_events []*health.Event) []ical.CalendarEvent {
	var calendar_events []ical.CalendarEvent
	for _, health_event := range health_events {
		health_arn := health_event.Arn
		event_affected_resources, event_description, full_event_type, event_service, event_start_time, event_end_time := process_event(health_arn)

		event_type := full_event_type

		switch full_event_type {
		case "AWS_EC2_INSTANCE_REBOOT_MAINTENANCE_SCHEDULED":
			event_type = "REBOOT"
		case "AWS_EC2_INSTANCE_POWER_MAINTENANCE_SCHEDULED":
			event_type = "REBOOT"
		case "AWS_EC2_SYSTEM_REBOOT_MAINTENANCE_SCHEDULED":
			event_type = "REBOOT"
		case "AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED":
			event_type = "RETIREMENT"
		case "AWS_EC2_INSTANCE_NETWORK_MAINTENANCE_SCHEDULED":
			event_type = "NET MAINT"
		case "AWS_RDS_MAINTENANCE_SCHEDULED":
			event_type = "MAINT SCHEDULED"
		}

		calendar_event_id := *health_arn
		calendar_event_description := event_description
		calendar_event_start_time := event_start_time
		calendar_event_end_time := event_end_time
		calendar_event_location := *health_event.Region

		for _, event_affected_resource := range event_affected_resources {
			var calendar_event_summary string
			switch event_service {
			case "EC2":
				calendar_event_summary = event_type + " " + ec2InstanceName(event_affected_resource, *health_event.Region) + " " + event_affected_resource
			case "RDS":
				calendar_event_summary = event_service + " " + event_affected_resource + " " + event_type
				calendar_event_start_time_rds, calendar_event_end_time_rds := maintenance_time(calendar_event_start_time, rds_maintenance_window(event_affected_resource, *health_event.Region))
				calendar_event_start_time = &calendar_event_start_time_rds
				calendar_event_end_time = &calendar_event_end_time_rds
			case "ELASTICACHE":
				calendar_event_summary = event_service + " " + event_affected_resource + " " + event_type
				calendar_event_start_time_elasticache, calendar_event_end_time_elasticache := maintenance_time(calendar_event_start_time, elasticache_maintenance_window(event_affected_resource, *health_event.Region))
				calendar_event_start_time = &calendar_event_start_time_elasticache
				calendar_event_end_time = &calendar_event_end_time_elasticache
			default:
				calendar_event_summary = event_service + " " + event_affected_resource + " " + event_type
			}
			calendar_event := calendar_event(calendar_event_id + "_" + event_affected_resource, calendar_event_summary, calendar_event_description, calendar_event_location, calendar_event_start_time, calendar_event_end_time)
			calendar_events = append(calendar_events, calendar_event)
		}
	}
	return calendar_events
}

func maintenance_time(event_start *time.Time, maintenance_window string) (time.Time, time.Time) {

	//parsing maintenance time
	//maintenance_window is in string format ddd:hh24:mi-ddd:hh24:mi
	maintenance_window_start_end := strings.Split(maintenance_window, "-")
	maintenance_window_start := strings.Split(maintenance_window_start_end[0], ":")
	maintenance_window_end := strings.Split(maintenance_window_start_end[1], ":")
	// maintenance_window_start[0] for weekday
	// maintenance_window_start[1] for hour
	// maintenance_window_start[2] for minute

	maintenance_window_start_time, maintenance_window_end_time := next_maintenance_window(event_start, maintenance_window_start, maintenance_window_end)

	return maintenance_window_start_time, maintenance_window_end_time
}

func next_maintenance_window(base_time *time.Time, maintenance_window_start []string, maintenance_window_end []string) (time.Time, time.Time) {
	maintenance_window_start_weekday := weekday_from_shortname(maintenance_window_start[0])
	maintenance_window_start_hour_int64,_ := strconv.ParseInt(maintenance_window_start[1], 10, 8)
	maintenance_window_start_hour := int(maintenance_window_start_hour_int64)
	maintenance_window_start_minute_int64,_ := strconv.ParseInt(maintenance_window_start[2], 10, 8)
	maintenance_window_start_minute := int(maintenance_window_start_minute_int64)

	maintenance_window_end_weekday := weekday_from_shortname(maintenance_window_end[0])
	maintenance_window_end_hour_int64,_ := strconv.ParseInt(maintenance_window_end[1], 10, 8)
	maintenance_window_end_hour := int(maintenance_window_end_hour_int64)
	maintenance_window_end_minute_int64,_ := strconv.ParseInt(maintenance_window_end[2], 10, 8)
	maintenance_window_end_minute := int(maintenance_window_end_minute_int64)

	days_ahead := maintenance_window_start_weekday - base_time.Weekday()
	if days_ahead < 0 {
		days_ahead += 7
	} else if days_ahead == 0 && maintenance_window_start_hour < base_time.Hour() && maintenance_window_start_minute < base_time.Minute(){
		days_ahead += 7
	}
	next_date := base_time.AddDate(0,0,int(days_ahead))
	next_maintenance_window_start := time.Date(next_date.Year(), next_date.Month(), next_date.Day(), maintenance_window_start_hour, maintenance_window_start_minute, 0, 0, next_date.Location())

	next_maintenance_window_end := next_maintenance_window_start
	if maintenance_window_end_weekday != maintenance_window_start_weekday {
		next_maintenance_window_end = next_maintenance_window_end.AddDate(0,0,int(1))
	}
	next_maintenance_window_end = time.Date(next_maintenance_window_end.Year(), next_maintenance_window_end.Month(), next_maintenance_window_end.Day(), maintenance_window_end_hour, maintenance_window_end_minute, 0, 0, next_date.Location())
	return next_maintenance_window_start, next_maintenance_window_end
}

func weekday_from_shortname(shortname string) time.Weekday {
	var weekday time.Weekday
	switch shortname {
	case "sun":
		weekday = time.Weekday(0)
	case "mon":
		weekday = time.Weekday(1)
	case "tue":
		weekday = time.Weekday(2)
	case "wed":
		weekday = time.Weekday(3)
	case "thu":
		weekday = time.Weekday(4)
	case "fri":
		weekday = time.Weekday(5)
	case "sat":
		weekday = time.Weekday(6)
	}
	return weekday
}

