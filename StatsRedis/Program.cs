using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SimpleSpeedTester.Core;
using StackExchange.Redis;

namespace StatsRedis {
	class Program {

		private static ConnectionMultiplexer _redis;
		private static int _timeStamp;
		private static IDatabase _db;
		private static string[] _data;
		private const string StatsKey = "statistics";
		private const string TestResultsKey = "results";
		private const int SecondsToRun = 20;

		private const int AppCount = 600;
		private const int ConcurrencyLevel = 10;

		static void Main(string[] args) {
			_redis = ConnectionMultiplexer.Connect("localhost");
			_timeStamp = (Int32)(new DateTime(2015, 03, 17).Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
			_data = GetData().OrderBy(x=> Guid.NewGuid()).ToArray();
			_db = _redis.GetDatabase();

			var summary = new TestGroup("RedisStats").PlanAndExecute("Test", DoWork, 1);
			Console.WriteLine(summary);
			
			PrintStatsFromDb();

			Console.WriteLine("Done...");
			Console.Read();
		}

		static void PrintStatsFromDb() {
			_db = _redis.GetDatabase();
			var data = _db.HashGetAll(StatsKey);

			var totalIncrementsInDb = data.Sum(x => (int)x.Value);

			Console.WriteLine("Total increments sent: {0}", _db.HashGet(TestResultsKey, "inc_sent"));
			Console.WriteLine("Total increments in db: {0}", totalIncrementsInDb);
			Console.WriteLine("Avarage per app: {0}", data.Average(x => (int)x.Value));
			foreach (var d in data)
			{
				Console.WriteLine(d);
			}
		}
		static void DoWork() {

			var rnd = new Random((int)DateTime.Now.Ticks);
			var cts = new CancellationTokenSource();
			cts.CancelAfter(SecondsToRun * 1000);

			int totalIncrements = 0;

			var tasks = new Task[ConcurrencyLevel];

			for (var i = 0; i < ConcurrencyLevel; i++) {

				tasks[i] = Task.Factory.StartNew(x => {
					var d = (string[])x;
					while (!cts.Token.IsCancellationRequested) {

						_db.HashIncrement(StatsKey, d[rnd.Next(0, AppCount - 1)]);
						Interlocked.Add(ref totalIncrements, 1);
					}

				}, _data, cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);

			}

			Task.WaitAll(tasks);

			_db = _redis.GetDatabase();
			_db.HashIncrement(TestResultsKey, "inc_sent", totalIncrements);

		}
		static IEnumerable<string> GetData() {
			return Enumerable.Range(0, AppCount).Select(x => string.Format("application_id:{0}:{1}", x.ToString("D"), _timeStamp.ToString("D"))).ToArray();
		}
	}
}
