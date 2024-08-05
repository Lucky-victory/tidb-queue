export interface Job<P = any> {
  id: number;
  queueName: string;
  payload: P | null;
  status: "queued" | "processing" | "completed" | "failed";
  runAt: Date;
  priority: number;
  attempts: number;
  jobId: string;
  delay: string;
  maxAttempts: number;
  retryOnFail: boolean;
  retryOnFailInterval: string;
  maxRetryOnFailAttempts: number;
  retryOnFailAttempts: number;
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
   * The maximum number of retry attempts when the job fails, default is 3
   */
  maxRetryOnFailAttempts?: number;
  /**
   * The interval between retry attempts when the job fails, default is 5s
   */
  retryOnFailInterval?: string;

  /**
   * The cron expression for scheduling the job, e.g. "0 0 * * *" for every midnight
   */
  cron?: string;
}

export interface QueueOptions {
  /**
   * How frequently should the jobs be checked,  (default is 5s)
   */
  pollingInterval?: string;
  /**
   * How many concurrent jobs to run at once,(default is 5)
   */
  concurrency?: number;
  /**
   * Whether to automatically start the job queue, (default is true)
   */
  autoStart?: boolean;
  /**
   * Whether to use a separate process for running jobs, (default is false)
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
