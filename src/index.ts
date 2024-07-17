import { EventEmitter } from "events";
import * as mysql from "mysql2/promise";
import { fork, ChildProcess } from "child_process";
import { CronJob } from "cron";
import * as winston from "winston";
import * as path from "path";
import { v4 as uuid } from "uuid";
import isEmpty from "just-is-empty";

export interface Job<P = any> {
  id: number;
  type: string;
  payload: P;
  status: "queued" | "processing" | "completed" | "failed";
  runAt: Date;
  priority: number;
  attempts: number;

  jobId: string;

  delay: string;

  maxAttempts: number;

  retryOnFail: boolean;

  retryInterval: string;

  cron: string;
}

export interface JobOptions {
  /**
   * Set a custom id for the job (not more than 36 characters), e.g uuid
   */
  jobId?: string;
  /**
   * The delay before the job starts in this format 1s, 2m, 1h
   */
  delay?: string;

  priority?: number;
  /**
   * The maximum number of attempts to run the job, default 3
   */
  maxAttempts?: number;
  /**
   * Whether to retry the job when it fails, default true
   */
  retryOnFail?: boolean;
  /**
   * the delay before the next retry in this format 1s, 2m, 1h
   */
  retryDelay?: string;

  /**
   * The cron expression for scheduling the job, e.g. "0 0 * * *" for every midnight
   */
  cron?: string;
}

export interface JobQueueOptions {
  /**
   * How frequently should the jobs be checked
   */
  pollingInterval?: string;
  /**
   * How many concurrent jobs to run at once, default is 5
   */
  concurrency?: number;
  /**
   * Whether to automatically start the job queue, default is true
   */
  autoStart?: boolean;
  /**
   * Whether to use a separate process for running jobs, default is false
   */
  useSeparateProcess?: boolean;
  /**
   * The path to the worker file if useSeparateProcess is true
   * */
  workerFile?: string;
  /**
   * Whether to enable logging to the stdout and a file or not.
   */
  logging?: {
    enabled: boolean;

    level?: string;
    /**
     * Logs filename, default is job_queue.log
     */
    filename?: string;
  };
}

export class JobQueue extends EventEmitter {
  private pool: mysql.Pool;
  private pollingInterval: number;
  private concurrency: number;
  private isRunning: boolean = false;
  private isPaused: boolean = false;
  private activeJobs: number = 0;
  private cronJobs: Map<number, CronJob> = new Map();
  private workers: ChildProcess[] = [];
  private logger?: winston.Logger;
  private useSeparateProcess: boolean;
  private workerFile?: string;
  private pollIntervalId?: NodeJS.Timeout;

  constructor(dbConfig: mysql.PoolOptions, options: JobQueueOptions = {}) {
    super();
    this.pool = mysql.createPool(dbConfig);
    this.pollingInterval = this.parseTime(options.pollingInterval || "5s");
    this.concurrency = options.concurrency || 5;
    this.useSeparateProcess = options.useSeparateProcess || false;
    this.workerFile = options.workerFile;

    // Setup logging if enabled
    if (options.logging && options.logging.enabled) {
      this.logger = winston.createLogger({
        level: options.logging.level || "info",
        format: winston.format.simple(),
        transports: [
          new winston.transports.Console(),
          new winston.transports.File({
            filename: options.logging.filename || "job_queue.log",
          }),
        ],
      });
    }

    // Call init in constructor
    this.initPromise = this.init();

    if (options.autoStart) {
      this.initPromise.then(() => this.start());
    }
  }

  private parseTime(time: string): number {
    const secs = 1000;
    const units: { [key: string]: number } = {
      s: secs,
      m: secs * 60,
      h: secs * 60 * 60,
      d: secs * 60 * 60 * 24,
    };
    const match = time.match(/^(\d+)([smhd])$/);
    if (!match)
      throw new Error(
        "Invalid time format. Use <number><unit>, e.g., 30s, 5m, 2h, 1d"
      );
    return parseInt(match[1]) * units[match[2]];
  }

  private initPromise: Promise<void>;

  /**
   * Initialize the job queue by creating necessary database tables
   */

  private async init(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        type VARCHAR(50) NOT NULL,
        jobId CHAR(36) DEFAULT UUID(),
        payload JSON,
        status ENUM('queued', 'processing', 'completed', 'failed') DEFAULT 'queued',
        run_at TIMESTAMP NOT NULL,
        priority INT DEFAULT 0,
        attempts INT DEFAULT 0,
        max_attempts INT DEFAULT 3,
        cron VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
      )
    `);
    await this.pool.query(
      "CREATE INDEX IF NOT EXISTS idx_status_run_at_priority_job_id ON jobs (status, run_at, priority,jobId)"
    );
  }

  /**
   * Add a new job to the queue
   */
  async addJob(
    type: string,
    payload: any,
    options: JobOptions = {}
  ): Promise<number> {
    await this.initPromise;
    const {
      delay = "0s",
      priority = 0,
      maxAttempts = 3,
      cron,
      jobId: customJobId,
    } = options;
    const runAt = new Date(Date.now() + this.parseTime(delay));
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      "INSERT INTO jobs (jobId,type, payload, run_at, priority, max_attempts, cron) VALUES (?,?, ?, ?, ?, ?, ?)",
      [
        !isEmpty(customJobId) ? customJobId : uuid(),
        type,
        JSON.stringify(payload),
        runAt,
        priority,
        maxAttempts,
        cron,
      ]
    );

    const jobId = result.insertId;

    if (cron) {
      this.setupCronJob(jobId, cron);
    }

    return jobId;
  }

  /**
   * Setup a cron job for repeated execution
   */
  private setupCronJob(jobId: number, cronExpression: string): void {
    const job = new CronJob(cronExpression, () => this.triggerCronJob(jobId));
    this.cronJobs.set(jobId, job);
    job.start();
  }

  /**
   * Trigger a cron job by resetting its run time
   */
  private async triggerCronJob(jobId: number): Promise<void> {
    await this.pool.query(
      'UPDATE jobs SET run_at = NOW(), attempts = 0 WHERE id = ? AND status != "processing"',
      [jobId]
    );
  }

  /**
   * Start the job queue
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.isPaused = false;
    this.pollIntervalId = setInterval(
      () => this.processJobs(),
      this.pollingInterval
    );
    this.emit("start");
    this.log("info", "Job queue started");
  }

  /**
   * Stop the job queue
   */
  stop(): void {
    if (!this.isRunning) return;
    this.isRunning = false;
    if (this.pollIntervalId) {
      clearInterval(this.pollIntervalId);
    }
    this.cronJobs.forEach((job) => job.stop());
    this.cronJobs.clear();
    this.workers.forEach((worker) => worker.kill());
    this.workers = [];
    this.emit("stop");
    this.log("info", "Job queue stopped");
  }

  /**
   * Pause the job queue
   */
  pause(): void {
    this.isPaused = true;
    this.emit("pause");
    this.log("info", "Job queue paused");
  }

  /**
   * Resume the job queue
   */
  resume(): void {
    this.isPaused = false;
    this.emit("resume");
    this.log("info", "Job queue resumed");
  }

  /**
   * Process queued jobs
   */
  private async processJobs(): Promise<void> {
    if (this.isPaused || this.activeJobs >= this.concurrency) return;

    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();

      const [_jobs] = await connection.query(
        `
        SELECT * FROM jobs 
        WHERE status = 'queued' AND run_at <= NOW()
        ORDER BY priority DESC, run_at
        LIMIT ?
        FOR UPDATE SKIP LOCKED
      `,
        [this.concurrency - this.activeJobs]
      );
      const jobs = _jobs as Job[];
      for (const job of jobs) {
        await connection.query(
          'UPDATE jobs SET status = "processing", attempts = attempts + 1 WHERE id = ?',
          [job.id]
        );
      }

      await connection.commit();

      for (const job of jobs) {
        this.activeJobs++;
        this.emit("jobStart", job);
        this.log("info", `Starting job ${job.jobId} of type ${job.type}`);
        this.processJob(job);
      }
    } catch (error) {
      await connection.rollback();
      this.log("error", "Error processing jobs:", error);
      this.emit("error", error);
    } finally {
      connection.release();
    }
  }

  /**
   * Process a single job
   */
  private processJob(job: Job): void {
    if (this.useSeparateProcess) {
      this.processJobInSeparateProcess(job);
    } else {
      this.processJobInline(job);
    }
  }

  /**
   * Process a job in a separate process
   */
  private processJobInSeparateProcess(job: Job): void {
    if (!this.workerFile) {
      throw new Error("Worker file path not specified");
    }
    const worker = fork(this.workerFile);
    this.workers.push(worker);

    worker.on(
      "message",
      async (message: { status: string; result?: any; error?: Error }) => {
        try {
          if (message.status === "completed") {
            await this.completeJob(job, message.result);
          } else if (message.status === "failed") {
            await this.handleFailedJob(job, message?.error!);
          }
        } catch (error) {
          this.log(
            "error",
            `Error handling job ${job.jobId} completion:`,
            error
          );
          this.emit("error", error);
        } finally {
          this.activeJobs--;
          const index = this.workers.indexOf(worker);
          if (index > -1) {
            this.workers.splice(index, 1);
          }
          worker.kill();
        }
      }
    );

    worker.on("error", (error) => {
      this.log("error", `Worker error for job ${job.jobId}:`, error);
      this.emit("error", error);
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        this.log(
          "warn",
          `Worker for job ${job.jobId} exited with code ${code}`
        );
      }
    });

    worker.send(job);
  }

  /**
   * Process a job inline (in the same process)
   */
  private async processJobInline(job: Job): Promise<void> {
    try {
      const result = await this.executeJob(job);
      await this.completeJob(job, result);
    } catch (error) {
      await this.handleFailedJob(job, error as Error);
    } finally {
      this.activeJobs--;
    }
  }

  /**
   * Execute a job (to be implemented by the user)
   * The return value is optional and can be used for complex workflows or logging
   */
  async executeJob(job: Job): Promise<any> {
    // This method should be overridden by the user
    throw new Error("executeJob method not implemented");
  }

  /**
   * Complete a job
   */
  private async completeJob(job: Job, result: any): Promise<void> {
    await this.pool.query('UPDATE jobs SET status = "completed" WHERE id = ?', [
      job.id,
    ]);
    this.log("info", `Job ${job.jobId} completed successfully`);
    this.emit("completed", job, result);
  }

  /**
   * Handle a failed job
   */
  private async handleFailedJob(job: Job, error: Error): Promise<void> {
    if (job.attempts >= job.maxAttempts) {
      await this.pool.query('UPDATE jobs SET status = "failed" WHERE id = ?', [
        job.id,
      ]);
      this.log(
        "warn",
        `Job ${job.jobId} failed after ${job.attempts} attempts`
      );
      this.emit("failed", job, error);
    } else if (job.attempts >= job.maxAttempts && job.retryOnFail) {
      await this.retryJob(job.id);
      this.emit("retry", job, error);
    } else {
      const nextRunAt = new Date(
        Date.now() + this.parseTime(job.retryInterval)
      );
      await this.pool.query(
        'UPDATE jobs SET status = "queued", run_at = ? WHERE id = ?',
        [nextRunAt, job.id]
      );
      this.log("info", `Scheduling retry for job ${job.jobId}`);
      this.emit("retry", job, error);
    }
  }

  /**
   * Get the status of a job
   */
  async getJobStatus(jobId: number): Promise<Job | null> {
    const [_jobs] = await this.pool.query("SELECT * FROM jobs WHERE id = ?", [
      jobId,
    ]);
    const jobs = _jobs as Job[];
    return jobs[0] || null;
  }

  /**
   * Cancel a job
   */
  async cancelJob(jobId: number): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "failed" WHERE id = ? AND status IN ("queued", "processing")',
      [jobId]
    );
    return result.affectedRows > 0;
  }

  /**
   * Retry a failed job
   */
  async retryJob(jobId: number): Promise<boolean> {
    const [result] = await this.pool.query<mysql.ResultSetHeader>(
      'UPDATE jobs SET status = "queued", attempts = 0, run_at = NOW() WHERE id = ? AND status = "failed"',
      [jobId]
    );
    return result.affectedRows > 0;
  }

  /**
   * Get queue statistics
   */
  async getQueueStats(): Promise<{
    queued: number;
    processing: number;
    completed: number;
    failed: number;
  }> {
    const [_rows] = await this.pool.query(`
      SELECT status, COUNT(*) as count
      FROM jobs
      GROUP BY status
    `);
    const rows = _rows as {
      status: "queued" | "completed" | "processing" | "failed";
      count: number;
    }[];
    const stats = { queued: 0, processing: 0, completed: 0, failed: 0 };
    rows.forEach((row) => {
      stats[row.status as keyof typeof stats] = row.count;
    });

    return stats;
  }

  /**
   * Clean up old jobs
   */
  async cleanup(daysToKeep: number = 30): Promise<void> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

    await this.pool.query(
      `
      DELETE FROM jobs 
      WHERE (status = 'completed' OR status = 'failed') 
      AND updated_at < ?
      AND cron IS NULL
    `,
      [cutoffDate]
    );
  }

  /**
   * Log a message if logging is enabled
   */
  private log(level: string, message: string, meta?: any): void {
    if (this.logger) {
      this.logger.log(level, message, meta);
    }
  }

  /**
   * Clean up resources when shutting down
   */
  async shutdown(): Promise<void> {
    this.stop();
    await this.pool.end();
  }
}

export default JobQueue;
