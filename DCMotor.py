import RPi.GPIO as GPIO
#import time

# dc motor class to start stop motor
class DCMotor:
    # Pins for Motor Driver Inputs
    __Motor1A = 24
    __Motor1B = 23
    __Motor1E = 25

    # dc motor class init method or constructor
    def __init__(self):
        # setup motor driver pins
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)  # GPIO Numbering
        GPIO.setup(DCMotor.__Motor1A, GPIO.OUT)  # All pins as Outputs
        GPIO.setup(DCMotor.__Motor1B, GPIO.OUT)
        GPIO.setup(DCMotor.__Motor1E, GPIO.OUT)

    def start(self):
        # Going forwards
        GPIO.output(DCMotor.__Motor1A, GPIO.LOW)
        GPIO.output(DCMotor.__Motor1B, GPIO.HIGH)
        GPIO.output(DCMotor.__Motor1E, GPIO.HIGH)

    def stop(self):
        # stop
        GPIO.output(DCMotor.__Motor1E, GPIO.LOW)

# if __name__ == '__main__':
#     motor = DCMotor()
#     motor.start()
#     time.sleep(5)
#     motor.stop()