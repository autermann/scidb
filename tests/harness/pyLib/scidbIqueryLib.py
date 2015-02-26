import subprocess

class scidbIquery:
    def __init__(self,iqueryPath='iquery'):
        self.__iquery = iqueryPath

    def runQuery(
        self,
        query,
        opts=['-aq'],
        stdoutOpt=subprocess.PIPE,
        stderrOpt=subprocess.PIPE,
        ):
        cmd = [self.__iquery]
        cmd.extend(opts)
        cmd.append(query)

        process = subprocess.Popen(
            cmd,
            stdout=stdoutOpt,
            stderr=stderrOpt
            )
        exitCode = process.wait()
        stdoutData,stderrData = process.communicate(None)
        return exitCode,stdoutData,stderrData

