import * as winston from 'winston';
export type Logger=winston.Logger
export const logger=({level,filename,queueName}:{level?:string,filename?:string,queueName:string})=>winston.createLogger({
        level: level || "info",
        format: winston.format.simple(),
        transports: [
          new winston.transports.Console(),
          new winston.transports.File({
            filename:filename || `${queueName}.log`,
          }),
        ],
      });